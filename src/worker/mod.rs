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

pub mod analyze;
pub mod audit;
pub mod buffer;
pub mod cover;
pub mod import;
pub mod metadata;
pub mod release;
pub mod source;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};

/// Adaptive rate limiter for Archive.org API requests.
///
/// Starts at a conservative rate (15 req/min) and adjusts based on errors:
/// - On success: gradually speeds up (up to 2x baseline)
/// - On rate limit error: backs off exponentially
/// - Shared globally across all jobs
pub struct AdaptiveRateLimiter {
    /// Base interval between requests (ms) - 15 req/min = 4000ms
    base_interval_ms: u64,
    /// Current interval between requests (ms) - adjusted dynamically
    current_interval_ms: AtomicU64,
    /// Minimum interval (fastest we'll ever go) - 2000ms = 30 req/min
    min_interval_ms: u64,
    /// Maximum interval (slowest, after many errors) - 60000ms = 1 req/min
    max_interval_ms: u64,
    /// Last request time
    last_request: Mutex<Instant>,
    /// Consecutive successes (for speed-up)
    consecutive_successes: AtomicU64,
    /// Consecutive errors (for backoff)
    consecutive_errors: AtomicU64,
}

impl AdaptiveRateLimiter {
    /// Create a new adaptive rate limiter.
    ///
    /// # Arguments
    /// * `requests_per_minute` - Baseline requests per minute (default: 15)
    pub fn new(requests_per_minute: u32) -> Arc<Self> {
        let base_interval_ms = (60_000 / requests_per_minute.max(1)) as u64;

        info!(
            requests_per_minute = requests_per_minute,
            base_interval_ms = base_interval_ms,
            "Adaptive rate limiter initialized"
        );

        Arc::new(Self {
            base_interval_ms,
            current_interval_ms: AtomicU64::new(base_interval_ms),
            min_interval_ms: base_interval_ms / 2, // Can speed up to 2x
            max_interval_ms: 60_000, // Slowest: 1 req/min
            last_request: Mutex::new(Instant::now() - Duration::from_secs(10)), // Allow immediate first request
            consecutive_successes: AtomicU64::new(0),
            consecutive_errors: AtomicU64::new(0),
        })
    }

    /// Wait until we're allowed to make the next request.
    pub async fn wait(&self) {
        let mut last = self.last_request.lock().await;
        let interval = Duration::from_millis(self.current_interval_ms.load(Ordering::Relaxed));
        let elapsed = last.elapsed();

        if elapsed < interval {
            let wait_time = interval - elapsed;
            debug!(
                wait_ms = wait_time.as_millis(),
                interval_ms = interval.as_millis(),
                "Rate limiting: waiting before Archive.org request"
            );
            tokio::time::sleep(wait_time).await;
        }

        *last = Instant::now();
    }

    /// Report a successful request - may speed up the rate.
    pub fn report_success(&self) {
        self.consecutive_errors.store(0, Ordering::Relaxed);
        let successes = self.consecutive_successes.fetch_add(1, Ordering::Relaxed) + 1;

        // Speed up after 10 consecutive successes
        if successes >= 10 && successes % 10 == 0 {
            let current = self.current_interval_ms.load(Ordering::Relaxed);
            let new_interval = (current * 9 / 10).max(self.min_interval_ms); // 10% faster

            if new_interval < current {
                self.current_interval_ms.store(new_interval, Ordering::Relaxed);
                debug!(
                    old_interval_ms = current,
                    new_interval_ms = new_interval,
                    successes = successes,
                    "Rate limiter speeding up after successes"
                );
            }
        }
    }

    /// Report a rate limit error - backs off exponentially.
    pub fn report_rate_limit_error(&self) {
        self.consecutive_successes.store(0, Ordering::Relaxed);
        let errors = self.consecutive_errors.fetch_add(1, Ordering::Relaxed) + 1;

        let current = self.current_interval_ms.load(Ordering::Relaxed);
        // Double the interval on each error, up to max
        let new_interval = (current * 2).min(self.max_interval_ms);

        self.current_interval_ms.store(new_interval, Ordering::Relaxed);

        warn!(
            old_interval_ms = current,
            new_interval_ms = new_interval,
            consecutive_errors = errors,
            "Rate limiter backing off after rate limit error"
        );
    }

    /// Report a transient error (not rate limiting) - slight backoff.
    pub fn report_transient_error(&self) {
        let current = self.current_interval_ms.load(Ordering::Relaxed);
        // 25% slower on transient errors
        let new_interval = (current * 5 / 4).min(self.max_interval_ms);

        if new_interval > current {
            self.current_interval_ms.store(new_interval, Ordering::Relaxed);
            debug!(
                old_interval_ms = current,
                new_interval_ms = new_interval,
                "Rate limiter slowing after transient error"
            );
        }
    }

    /// Get current requests per minute rate.
    pub fn current_rate_per_minute(&self) -> f64 {
        let interval_ms = self.current_interval_ms.load(Ordering::Relaxed);
        60_000.0 / interval_ms as f64
    }
}

/// Type alias for the adaptive rate limiter
pub type ArchiveRateLimiter = Arc<AdaptiveRateLimiter>;

/// Create an adaptive rate limiter for Archive.org API requests.
/// Default: 15 requests per minute, adapts based on errors.
pub fn create_archive_rate_limiter(requests_per_minute: u32) -> ArchiveRateLimiter {
    AdaptiveRateLimiter::new(requests_per_minute)
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

    /// Archive.org rate limit (requests per minute).
    /// Default: 15 (conservative baseline, adapts based on errors)
    pub archive_requests_per_minute: u32,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            archivist_url: "http://localhost:8080".to_string(),
            lens_url: None,
            auth: None,
            buffer: BufferPoolConfig::default(),
            archive_requests_per_minute: 15, // 15 req/min baseline, adapts to errors
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
            .field("archive_requests_per_minute", &self.archive_requests_per_minute)
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
        let archive_rate_limiter = create_archive_rate_limiter(config.archive_requests_per_minute);
        info!(
            requests_per_minute = config.archive_requests_per_minute,
            "Archive.org adaptive rate limiter initialized"
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
            JobType::Analyze => self.execute_analyze(&job).await,
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
            ref audio_quality,
            ref quality_tiers,
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
                audio_quality.as_ref(),
                Some(quality_tiers),
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

    /// Execute an audit job - scans Citadel Lens for quality issues.
    async fn execute_audit(&self, job: &Job) -> anyhow::Result<JobResult> {
        let job_id_hex = hex::encode(job.id.as_bytes());

        // Check if Lens URL is configured
        let lens_url = match &self.config.lens_url {
            Some(url) if !url.is_empty() => url.as_str(),
            _ => {
                warn!("No Citadel Lens URL configured - cannot run audit. Set LENS_URL env var.");
                anyhow::bail!("No Citadel Lens URL configured");
            }
        };

        info!(job_id = %job_id_hex, lens_url = %lens_url, "Starting audit of Citadel Lens releases");

        // Run the audit
        let (total_releases, releases_with_issues, audits, issue_counts) = audit::run_audit(
            &self.state.http_client,
            lens_url,
            |progress, message| {
                if let Some(msg) = message {
                    debug!(job_id = %job_id_hex, progress = %progress, message = %msg, "Audit progress");
                }
            },
        ).await?;

        info!(
            job_id = %job_id_hex,
            total = total_releases,
            with_issues = releases_with_issues,
            "Audit complete"
        );

        Ok(JobResult::Audit {
            total_releases,
            releases_with_issues,
            audits,
            issue_counts,
            // Legacy fields not used in new audits
            source_quality: None,
            missing_formats: None,
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

    /// Execute an analyze job - extracts metadata without re-uploading.
    async fn execute_analyze(&self, job: &Job) -> anyhow::Result<JobResult> {
        let (source, gateway, release_id) = match &job.target {
            JobTarget::Source {
                source,
                gateway,
                existing_release_id,
            } => (
                source.clone(),
                gateway.clone(),
                existing_release_id.clone().unwrap_or_default(),
            ),
            JobTarget::Release(id) => {
                // For release target, we need to fetch the release to get its CID
                if let Some(lens_url) = &self.config.lens_url {
                    let release_url = format!(
                        "{}/api/v1/releases/{}",
                        lens_url.trim_end_matches('/'),
                        id
                    );
                    match self.state.http_client.get(&release_url).send().await {
                        Ok(resp) if resp.status().is_success() => {
                            let release: serde_json::Value = resp.json().await?;
                            let cid = release
                                .get("contentCID")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| anyhow::anyhow!("Release has no contentCID"))?;
                            (cid.to_string(), None, id.clone())
                        }
                        _ => anyhow::bail!("Failed to fetch release {} from Citadel Lens", id),
                    }
                } else {
                    anyhow::bail!("Analyze job with Release target requires Lens URL");
                }
            }
            _ => anyhow::bail!("Analyze job requires Source or Release target"),
        };

        info!(source = %source, release_id = %release_id, "Analyzing content");

        let job_id_hex = hex::encode(job.id.as_bytes());
        let result = analyze::execute_analyze(
            &self.state.http_client,
            &source,
            &release_id,
            gateway.as_deref(),
            self.config.lens_url.as_deref(),
            |progress, message| {
                if let Some(msg) = message {
                    debug!(job_id = %job_id_hex, progress = %progress, message = %msg, "Analyze progress");
                }
            },
        )
        .await?;

        Ok(result)
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
