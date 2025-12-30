//! Event-driven job worker for background execution.
//!
//! The worker waits on a channel for job notifications - NO POLLING.
//! When a job is created, the API sends a notification and the worker
//! wakes immediately to execute it.
//!
//! File downloads run in PARALLEL across all jobs, controlled by the
//! BufferPool's semaphore. This means:
//! - An album with 10 tracks downloads 8 tracks concurrently (up to max_concurrent)
//! - Multiple jobs can run simultaneously, sharing the global concurrency limit
//! - BufferPool enforces the global file download limit across all jobs
//!
//! This is the correct pattern:
//! - Job created → notification sent → worker wakes → job spawned
//! - File downloads within jobs run in parallel (buffer_unordered)
//! - BufferPool semaphore throttles global concurrent file downloads
//! - Zero latency, zero wasted CPU cycles

pub mod buffer;
pub mod cover;
pub mod import;
pub mod release;
pub mod source;

use std::sync::Arc;
use std::time::SystemTime;

use governor::{Quota, RateLimiter, clock::SystemClock, state::{InMemoryState, direct::NotKeyed}, middleware::NoOpMiddleware};
use std::num::NonZeroU32;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Type alias for Archive.org rate limiter (global, shared across all downloads)
pub type ArchiveRateLimiter = Arc<RateLimiter<NotKeyed, InMemoryState, SystemClock, NoOpMiddleware<SystemTime>>>;

/// Create a rate limiter for Archive.org API requests.
/// Default: 2 requests per second (conservative, Archive.org has undocumented limits)
pub fn create_archive_rate_limiter(requests_per_second: u32) -> ArchiveRateLimiter {
    let quota = Quota::per_second(NonZeroU32::new(requests_per_second).unwrap_or(NonZeroU32::new(2).unwrap()));
    Arc::new(RateLimiter::direct_with_clock(quota, &SystemClock::default()))
}

use crate::api::{ApiState, JobNotification};
use crate::auth::Auth;
use crate::crdt::{Job, JobResult, JobTarget, JobType};

pub use buffer::{BufferPool, BufferPoolConfig, BufferSlot};

/// Configuration for the job worker.
#[derive(Clone)]
pub struct WorkerConfig {
    /// Archivist API URL for uploads.
    pub archivist_url: String,

    /// Citadel Lens API URL for creating releases.
    /// If None, releases won't be auto-created after import.
    pub lens_url: Option<String>,

    /// Authentication for Archivist uploads.
    /// If None, uploads will fail with 403.
    pub auth: Option<Auth>,

    /// Buffer pool configuration.
    pub buffer: BufferPoolConfig,

    /// Archive.org rate limit (requests per second).
    /// Default: 2 (conservative to be good citizens)
    pub archive_rate_limit: u32,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            archivist_url: "http://localhost:8080".to_string(),
            lens_url: None,
            auth: None,
            buffer: BufferPoolConfig::default(),
            archive_rate_limit: 2, // 2 requests/sec to Archive.org
        }
    }
}

impl std::fmt::Debug for WorkerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerConfig")
            .field("archivist_url", &self.archivist_url)
            .field("lens_url", &self.lens_url)
            .field("auth", &self.auth.as_ref().map(|a| a.public_key()))
            .field("buffer", &self.buffer)
            .field("archive_rate_limit", &self.archive_rate_limit)
            .finish()
    }
}

/// Event-driven job worker.
///
/// Waits on a channel for job notifications. When notified:
/// 1. Spawns a task to claim and execute the job
/// 2. File downloads run in parallel up to max_concurrent limit
/// 3. BufferPool's semaphore controls per-file concurrency globally
///
/// NO POLLING. NO SLEEPS. Pure event-driven with parallel file downloads.
pub struct JobWorker {
    state: Arc<ApiState>,
    config: WorkerConfig,
    /// Shared buffer pool for all downloads - controls global concurrency
    buffer_pool: Arc<BufferPool>,
    /// Global rate limiter for Archive.org API requests
    archive_rate_limiter: ArchiveRateLimiter,
}

impl JobWorker {
    /// Create a new job worker.
    pub fn new(state: Arc<ApiState>, config: WorkerConfig) -> Self {
        let buffer_pool = BufferPool::new(config.buffer.clone());
        let archive_rate_limiter = create_archive_rate_limiter(config.archive_rate_limit);
        info!(
            rate_limit = config.archive_rate_limit,
            "Archive.org rate limiter initialized"
        );
        Self { state, config, buffer_pool, archive_rate_limiter }
    }

    /// Run the worker - waits on channel for job notifications.
    ///
    /// Jobs are spawned as separate tasks and run in PARALLEL.
    /// A semaphore limits concurrent executions to max_concurrent.
    pub async fn run(self: Arc<Self>, mut job_rx: mpsc::Receiver<JobNotification>) {
        info!(
            max_concurrent = self.config.buffer.max_concurrent,
            "Job worker started (parallel execution enabled)"
        );

        // Wait for job notifications - blocks until message arrives
        while let Some(notification) = job_rx.recv().await {
            let job_id = notification.job_id;
            let job_id_hex = hex::encode(job_id.as_bytes());

            debug!(job_id = %job_id_hex, "Received job notification, spawning task");

            // Clone self for the spawned task
            let worker = Arc::clone(&self);

            // Spawn job execution as a separate task
            // The semaphore inside try_execute_job limits concurrency
            tokio::spawn(async move {
                if let Err(e) = worker.try_execute_job(&job_id).await {
                    error!(job_id = %job_id_hex, error = %e, "Failed to execute job");
                }
            });
        }

        info!("Job worker shutting down (channel closed)");
    }

    /// Try to claim and execute a job.
    /// File downloads within jobs are throttled by the buffer pool's semaphore.
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

            // Claim the job for this node (required before starting)
            node.claim_job(job_id)?;

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
            JobType::SourceImport => self.execute_source_import(&job).await,
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

        // Get auth credentials - warn if not configured
        let auth_creds = self.config.auth.as_ref().map(|a| a.create_credentials());
        if auth_creds.is_none() {
            warn!("No auth configured - uploads may fail with 403. Run 'librarian init' first.");
        }

        let job_id_hex = hex::encode(job.id.as_bytes());
        let mut result = import::execute_import(
            &self.state.http_client,
            &self.config.archivist_url,
            &identifier,
            auth_creds.as_ref(),
            &self.buffer_pool,
            &self.archive_rate_limiter,
            |progress, message| {
                if let Some(msg) = message {
                    debug!(job_id = %job_id_hex, progress = %progress, message = %msg, "Import progress");
                }
            },
        )
        .await?;

        // If import succeeded, try to process cover art and create release
        if let JobResult::Import {
            ref directory_cid,
            ref title,
            ref artist,
            ref source,
            ref license,
            ref date,
            ref track_metadata,
            ref mut thumbnail_cid,
            ..
        } = result
        {
            // Try to find and upload cover art
            // First, get the cover candidates from the Archive.org metadata
            // Note: We already found candidates during import, but we need to re-fetch
            // the metadata to get the file list for cover art processing
            let metadata_url = format!("https://archive.org/metadata/{}", identifier);
            if let Ok(metadata_resp) = self.state.http_client.get(&metadata_url).send().await {
                if let Ok(metadata) = metadata_resp.json::<serde_json::Value>().await {
                    // Extract files array and find cover candidates
                    if let Some(files) = metadata.get("files").and_then(|f| f.as_array()) {
                        let file_entries: Vec<import::ArchiveFileEntry> = files
                            .iter()
                            .filter_map(|f| serde_json::from_value(f.clone()).ok())
                            .collect();

                        let (_, candidates) = import::find_cover_candidates(&file_entries);

                        if !candidates.is_empty() {
                            info!(
                                identifier = %identifier,
                                candidates = candidates.len(),
                                "Processing cover art"
                            );

                            let cover_result = cover::find_and_upload_cover(
                                &self.state.http_client,
                                &self.config.archivist_url,
                                &identifier,
                                &candidates,
                                auth_creds.as_ref(),
                            )
                            .await;

                            if let Some(cid) = cover_result.thumbnail_cid {
                                info!(cid = %cid, confident = %cover_result.confident, "Cover art uploaded");
                                *thumbnail_cid = Some(cid);
                            }
                        }
                    }
                }
            }

            // Create release in Citadel Lens
            let release_id = release::try_create_release(
                &self.state.http_client,
                self.config.lens_url.as_deref(),
                title.as_deref(),
                artist.as_deref(),
                directory_cid,
                thumbnail_cid.as_deref(),
                source,
                license.as_deref(),
                date.as_deref(),
                track_metadata.as_deref(),
                self.config.auth.as_ref(),
            )
            .await;

            if let Some(id) = release_id {
                info!(release_id = %id, "Release created in Citadel Lens moderation queue");
            }
        }

        Ok(result)
    }

    /// Execute a source import job (URL/CID).
    async fn execute_source_import(&self, job: &Job) -> anyhow::Result<JobResult> {
        let (source, gateway, _existing_release_id) = match &job.target {
            JobTarget::Source {
                source,
                gateway,
                existing_release_id,
            } => (source.clone(), gateway.clone(), existing_release_id.clone()),
            _ => anyhow::bail!("SourceImport job requires Source target"),
        };

        info!(source = %source, "Importing from URL/CID");

        // Get auth credentials - warn if not configured
        let auth_creds = self.config.auth.as_ref().map(|a| a.create_credentials());
        if auth_creds.is_none() {
            warn!("No auth configured - uploads may fail with 403. Run 'librarian init' first.");
        }

        let job_id_hex = hex::encode(job.id.as_bytes());
        let result = source::execute_source_import(
            &self.state.http_client,
            &self.config.archivist_url,
            &source,
            gateway.as_deref(),
            auth_creds.as_ref(),
            &self.buffer_pool,
            |progress, message, speed| {
                if let Some(msg) = message {
                    if let Some(spd) = speed {
                        debug!(job_id = %job_id_hex, progress = %progress, message = %msg, speed = %spd, "Source import progress");
                    } else {
                        debug!(job_id = %job_id_hex, progress = %progress, message = %msg, "Source import progress");
                    }
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
    let worker = Arc::new(JobWorker::new(state, config));
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
