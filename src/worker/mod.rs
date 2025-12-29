//! Event-driven job worker for background execution.
//!
//! The worker waits on a channel for job notifications - NO POLLING.
//! When a job is created, the API sends a notification and the worker
//! wakes immediately to execute it.
//!
//! This is the correct pattern:
//! - Job created → notification sent → worker wakes → job executes
//! - Zero latency, zero wasted CPU cycles

pub mod import;

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::api::{ApiState, JobNotification};
use crate::crdt::{Job, JobResult, JobTarget, JobType};

/// Configuration for the job worker.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Maximum concurrent jobs.
    pub max_concurrent: usize,

    /// Archivist API URL for uploads.
    pub archivist_url: String,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 2,
            archivist_url: "http://localhost:8080".to_string(),
        }
    }
}

/// Event-driven job worker.
///
/// Waits on a channel for job notifications. When notified:
/// 1. Claims the job
/// 2. Executes based on job type
/// 3. Completes the job with result
///
/// NO POLLING. NO SLEEPS. Pure event-driven.
pub struct JobWorker {
    state: Arc<ApiState>,
    config: WorkerConfig,
}

impl JobWorker {
    /// Create a new job worker.
    pub fn new(state: Arc<ApiState>, config: WorkerConfig) -> Self {
        Self { state, config }
    }

    /// Run the worker - waits on channel for job notifications.
    ///
    /// This is the correct event-driven pattern:
    /// - Blocks on recv() until a job notification arrives
    /// - Wakes immediately when job is created
    /// - No polling interval, no sleep
    pub async fn run(&self, mut job_rx: mpsc::Receiver<JobNotification>) {
        info!("Job worker started (event-driven, waiting on channel)");

        // Wait for job notifications - blocks until message arrives
        while let Some(notification) = job_rx.recv().await {
            let job_id = notification.job_id;
            let job_id_hex = hex::encode(job_id.as_bytes());

            debug!(job_id = %job_id_hex, "Received job notification");

            // Try to claim and execute the job
            if let Err(e) = self.try_execute_job(&job_id).await {
                error!(job_id = %job_id_hex, error = %e, "Failed to execute job");
            }
        }

        info!("Job worker shutting down (channel closed)");
    }

    /// Try to claim and execute a job.
    async fn try_execute_job(&self, job_id: &citadel_crdt::ContentId) -> anyhow::Result<()> {
        let job_id_hex = hex::encode(job_id.as_bytes());

        // Claim and start the job
        let job = {
            let mut node = self.state.node.write().await;

            // Get the job
            let job = match node.get_job(job_id)? {
                Some(j) => j,
                None => {
                    debug!(job_id = %job_id_hex, "Job not found, may have been processed");
                    return Ok(());
                }
            };

            // Check if already completed
            if job.status != crate::crdt::JobStatus::Pending {
                debug!(job_id = %job_id_hex, status = ?job.status, "Job not pending, skipping");
                return Ok(());
            }

            // Start the job
            node.start_job(job_id).await?
        };

        info!(
            job_id = %job_id_hex,
            job_type = ?job.job_type,
            "Executing job"
        );

        // Execute based on job type
        let result = match job.job_type {
            JobType::Import => self.execute_import(&job).await,
            JobType::Audit => self.execute_audit(&job).await,
            JobType::Transcode => self.execute_transcode(&job).await,
            JobType::Migrate => self.execute_migrate(&job).await,
        };

        // Complete the job
        let job_result = match result {
            Ok(r) => r,
            Err(e) => {
                error!(job_id = %job_id_hex, error = %e, "Job execution failed");
                JobResult::Error(e.to_string())
            }
        };

        {
            let mut node = self.state.node.write().await;
            node.complete_job(job_id, job_result).await?;
        }

        info!(job_id = %job_id_hex, "Job completed");

        Ok(())
    }

    /// Execute an import job.
    async fn execute_import(&self, job: &Job) -> anyhow::Result<JobResult> {
        let identifier = match &job.target {
            JobTarget::ArchiveOrgItem(id) => id.clone(),
            _ => anyhow::bail!("Import job requires ArchiveOrgItem target"),
        };

        info!(identifier = %identifier, "Importing from Archive.org");

        let job_id_hex = hex::encode(job.id.as_bytes());
        let result = import::execute_import(
            &self.state.http_client,
            &self.config.archivist_url,
            &identifier,
            job.auth_pubkey.as_deref(),
            |progress, message| {
                if let Some(msg) = message {
                    debug!(job_id = %job_id_hex, progress = %progress, message = %msg, "Import progress");
                }
            },
        )
        .await?;

        Ok(result)
    }

    /// Execute an audit job (stub).
    async fn execute_audit(&self, _job: &Job) -> anyhow::Result<JobResult> {
        // TODO: Implement quality ladder audit
        Ok(JobResult::Audit {
            missing_formats: vec![],
            source_quality: "unknown".to_string(),
        })
    }

    /// Execute a transcode job (stub).
    async fn execute_transcode(&self, _job: &Job) -> anyhow::Result<JobResult> {
        // TODO: Implement FLAC -> Opus transcoding
        Ok(JobResult::Transcode { outputs: vec![] })
    }

    /// Execute a migrate job (stub).
    async fn execute_migrate(&self, _job: &Job) -> anyhow::Result<JobResult> {
        // TODO: Implement IPFS -> Archivist migration
        Ok(JobResult::Migrate {
            old_cid: "".to_string(),
            new_cid: "".to_string(),
            size: 0,
        })
    }
}

/// Spawn the event-driven job worker.
///
/// Returns the receiver end of the channel - pass to worker.run().
/// The sender is stored in ApiState for job creation notifications.
pub fn create_job_channel() -> (mpsc::Sender<JobNotification>, mpsc::Receiver<JobNotification>) {
    // Buffer of 100 should be plenty - jobs are processed quickly
    mpsc::channel(100)
}

/// Start the job worker in a background task.
pub fn spawn_worker(
    state: Arc<ApiState>,
    config: WorkerConfig,
    job_rx: mpsc::Receiver<JobNotification>,
) -> tokio::task::JoinHandle<()> {
    let worker = JobWorker::new(state, config);
    tokio::spawn(async move {
        worker.run(job_rx).await;
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_worker_receives_notification() {
        // Create channel
        let (tx, rx) = create_job_channel();

        // Worker should block on empty channel (we won't await it, just test channel works)
        let job_id = citadel_crdt::ContentId::hash(b"test-job");

        // Send notification
        tx.send(JobNotification { job_id: job_id.clone() })
            .await
            .expect("send should succeed");

        // Receive should get the notification
        let mut rx = rx;
        let notification = rx.recv().await.expect("should receive notification");
        assert_eq!(notification.job_id, job_id);
    }

    #[tokio::test]
    async fn test_channel_closes_cleanly() {
        let (tx, mut rx) = create_job_channel();

        // Drop sender
        drop(tx);

        // Receiver should return None (channel closed)
        assert!(rx.recv().await.is_none());
    }
}
