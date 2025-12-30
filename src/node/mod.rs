//! Embedded Citadel node for Librarian.
//!
//! This module provides an embedded Citadel node that handles:
//! - Job document storage and sync via citadel-docs
//! - Real-time gossip via citadel-gossip
//! - Peer health tracking via citadel-ping
//!
//! The node coordinates with other Librarian instances automatically
//! using SPORE for sync and TGP for bilateral coordination.

pub mod config;

use std::sync::Arc;
use std::time::Duration;

use citadel_crdt::ContentId;
use citadel_docs::DocumentStore;
use citadel_gossip::{GossipMessage, GossipPayload, GossipStore};
use citadel_ping::{Capability, PeerTracker, Ping};
use citadel_spore::U256;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::crdt::{Job, JobResult, JobStatus, JobTarget, JobType, UploadAuth};

pub use config::NodeConfig;

/// Errors that can occur in node operations.
#[derive(Error, Debug)]
pub enum NodeError {
    #[error("Storage error: {0}")]
    Storage(#[from] citadel_docs::DocError),

    #[error("Job not found: {0}")]
    JobNotFound(ContentId),

    #[error("Node not initialized")]
    NotInitialized,

    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("Invalid job state: {0}")]
    InvalidJobState(String),
}

pub type Result<T> = std::result::Result<T, NodeError>;

/// Job progress update for gossip broadcast.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct JobProgressUpdate {
    /// Job ID.
    pub job_id: ContentId,

    /// Progress percentage (0.0 - 1.0).
    pub progress: f32,

    /// Optional status message.
    pub message: Option<String>,
}

impl GossipPayload for JobProgressUpdate {
    const TOPIC: &'static str = "librarian/job_progress";
}

/// Embedded Citadel node for Librarian.
///
/// Combines document storage, gossip, and ping protocols into a single
/// coordinated node that syncs with other Librarian instances.
pub struct LibrarianNode {
    /// Node identity (256-bit).
    node_id: U256,

    /// Document store for jobs.
    store: DocumentStore,

    /// Gossip message store.
    gossip: GossipStore,

    /// Peer health tracker.
    peers: PeerTracker,

    /// Node capabilities.
    capabilities: Vec<Capability>,

    /// Current load (0.0 - 1.0).
    load: Arc<RwLock<f32>>,

    /// Running job count for load calculation.
    running_jobs: Arc<RwLock<usize>>,
}

impl LibrarianNode {
    /// Create a new Librarian node.
    pub fn new(config: NodeConfig) -> Result<Self> {
        // Generate or load node ID
        let node_id = config.node_id();

        // Open document store
        let store = DocumentStore::open(&config.data_dir)?;

        // Initialize gossip with subscriptions
        let mut gossip = GossipStore::new();
        gossip.subscribe(JobProgressUpdate::TOPIC);
        gossip.subscribe("librarian/admin");

        // Initialize peer tracker
        let peers = PeerTracker::with_timeouts(
            node_id,
            Duration::from_secs(60),  // stale timeout
            Duration::from_secs(15),  // ping interval
        );

        info!(node_id = %hex::encode(node_id.to_be_bytes()), "Librarian node initialized");

        Ok(Self {
            node_id,
            store,
            gossip,
            peers,
            capabilities: config.capabilities.clone(),
            load: Arc::new(RwLock::new(0.0)),
            running_jobs: Arc::new(RwLock::new(0)),
        })
    }

    /// Get the node's ID.
    pub fn node_id(&self) -> &U256 {
        &self.node_id
    }

    /// Get the node's capabilities.
    pub fn capabilities(&self) -> &[Capability] {
        &self.capabilities
    }

    /// Get current load.
    pub async fn load(&self) -> f32 {
        *self.load.read().await
    }

    // === Job Operations ===

    /// Create a new job.
    pub fn create_job(&mut self, job_type: JobType, target: JobTarget) -> Result<Job> {
        self.create_job_with_auth(job_type, target, None)
    }

    /// Create a new job with pre-authorized public key for uploads.
    pub fn create_job_with_auth(
        &mut self,
        job_type: JobType,
        target: JobTarget,
        auth_pubkey: Option<String>,
    ) -> Result<Job> {
        let auth = UploadAuth {
            pubkey: auth_pubkey,
            ..Default::default()
        };
        let job = Job::new_with_auth(job_type, target, auth);
        self.store.put(&job)?;

        debug!(job_id = %job.id, job_type = ?job.job_type, "Created new job");

        Ok(job)
    }

    /// Create a new job with full upload authorization (pubkey, signature, timestamp).
    pub fn create_job_with_auth_full(
        &mut self,
        job_type: JobType,
        target: JobTarget,
        auth: UploadAuth,
    ) -> Result<Job> {
        let job = Job::new_with_auth(job_type, target, auth);
        self.store.put(&job)?;

        debug!(job_id = %job.id, job_type = ?job.job_type, "Created new job with full auth");

        Ok(job)
    }

    /// Get a job by ID.
    pub fn get_job(&self, id: &ContentId) -> Result<Option<Job>> {
        Ok(self.store.get(id)?)
    }

    /// Update a job's auth pubkey (for pre-authorized uploads).
    pub fn update_job_auth(&mut self, id: &ContentId, auth_pubkey: String) -> Result<Job> {
        let mut job = self.store.get::<Job>(id)?
            .ok_or_else(|| NodeError::JobNotFound(id.clone()))?;

        job.auth_pubkey = Some(auth_pubkey);
        self.store.put(&job)?;

        debug!(job_id = %id, "Updated job auth pubkey");

        Ok(job)
    }

    /// List all jobs.
    pub fn list_jobs(&self) -> Result<Vec<Job>> {
        Ok(self.store.list()?)
    }

    /// List jobs by status.
    pub fn list_jobs_by_status(&self, status: JobStatus) -> Result<Vec<Job>> {
        let jobs = self.store.list::<Job>()?;
        Ok(jobs.into_iter().filter(|j| j.status == status).collect())
    }

    /// Claim a job for this node.
    pub fn claim_job(&mut self, id: &ContentId) -> Result<Job> {
        let mut job = self.store.get::<Job>(id)?
            .ok_or_else(|| NodeError::JobNotFound(id.clone()))?;

        job.claim(self.node_id);
        self.store.put(&job)?;

        debug!(job_id = %id, "Claimed job for this node");

        Ok(job)
    }

    /// Start executing a job.
    pub async fn start_job(&mut self, id: &ContentId) -> Result<Job> {
        let mut job = self.store.get::<Job>(id)?
            .ok_or_else(|| NodeError::JobNotFound(id.clone()))?;

        // Only start if we're the executor
        if !job.should_execute(&self.node_id) {
            warn!(
                job_id = %id,
                executor = ?job.executor(),
                my_id = %hex::encode(self.node_id.to_be_bytes()),
                "Cannot start job - not the assigned executor"
            );
            return Ok(job);
        }

        job.start();
        self.store.put(&job)?;

        // Update running job count
        {
            let mut count = self.running_jobs.write().await;
            *count += 1;
            self.update_load(*count).await;
        }

        debug!(job_id = %id, "Started job execution");

        Ok(job)
    }

    /// Complete a job with result.
    pub async fn complete_job(&mut self, id: &ContentId, result: JobResult) -> Result<Job> {
        let mut job = self.store.get::<Job>(id)?
            .ok_or_else(|| NodeError::JobNotFound(id.clone()))?;

        job.complete(result);
        self.store.put(&job)?;

        // Update running job count
        {
            let mut count = self.running_jobs.write().await;
            if *count > 0 {
                *count -= 1;
            }
            self.update_load(*count).await;
        }

        debug!(job_id = %id, status = ?job.status, "Completed job");

        Ok(job)
    }

    /// Retry a failed or completed job.
    /// Increments retry_count, resets status to Pending, clears claims and result.
    pub fn retry_job(&mut self, id: &ContentId) -> Result<Job> {
        let mut job = self.store.get::<Job>(id)?
            .ok_or_else(|| NodeError::JobNotFound(id.clone()))?;

        if !job.can_retry() {
            return Err(NodeError::InvalidJobState(format!(
                "Job {} is {:?}, cannot retry (must be Failed or Completed)",
                id, job.status
            )));
        }

        job.retry();
        self.store.put(&job)?;

        info!(
            job_id = %id,
            retry_count = job.retry_count,
            "Retried job - reset to Pending"
        );

        Ok(job)
    }

    /// Archive a completed or failed job (hide from main queue view).
    pub fn archive_job(&mut self, id: &ContentId) -> Result<Job> {
        let mut job = self.store.get::<Job>(id)?
            .ok_or_else(|| NodeError::JobNotFound(id.clone()))?;

        if !job.can_archive() {
            return Err(NodeError::InvalidJobState(format!(
                "Job {} cannot be archived (status: {:?}, archived: {})",
                id, job.status, job.archived
            )));
        }

        job.archive();
        self.store.put(&job)?;

        info!(job_id = %id, "Archived job");

        Ok(job)
    }

    /// Delete a job permanently from the store.
    pub fn delete_job(&mut self, id: &ContentId) -> Result<()> {
        let job = self.store.get::<Job>(id)?
            .ok_or_else(|| NodeError::JobNotFound(id.clone()))?;

        // Only allow deleting archived jobs
        if !job.archived {
            return Err(NodeError::InvalidJobState(format!(
                "Job {} must be archived before deletion",
                id
            )));
        }

        self.store.delete::<Job>(id)?;

        info!(job_id = %id, "Deleted archived job");

        Ok(())
    }

    /// Delete all archived jobs.
    pub fn delete_all_archived_jobs(&mut self) -> Result<usize> {
        let jobs = self.store.list::<Job>()?;
        let archived: Vec<_> = jobs.iter().filter(|j| j.archived).collect();
        let count = archived.len();

        for job in archived {
            self.store.delete::<Job>(&job.id)?;
        }

        info!(count = count, "Deleted all archived jobs");

        Ok(count)
    }

    /// Provide user metadata for a NeedsInput job and prepare it for retry.
    pub fn provide_job_metadata(
        &mut self,
        id: &ContentId,
        metadata: crate::crdt::ProvidedMetadata,
    ) -> Result<Job> {
        let mut job = self.store.get::<Job>(id)?
            .ok_or_else(|| NodeError::JobNotFound(id.clone()))?;

        // Verify job is in NeedsInput state
        match &job.result {
            Some(crate::crdt::JobResult::NeedsInput { .. }) => {
                // Good - this is what we expect
            }
            _ => {
                return Err(NodeError::InvalidJobState(format!(
                    "Job {} is not waiting for input",
                    id
                )));
            }
        }

        // Store the provided metadata
        job.provided_metadata = Some(metadata.clone());

        // Reset job for retry
        job.retry();

        self.store.put(&job)?;

        info!(
            job_id = %id,
            artist = %metadata.artist,
            album = %metadata.album,
            "Stored user-provided metadata and reset job for retry"
        );

        Ok(job)
    }

    /// Get jobs assigned to this node that are ready to execute.
    pub fn my_pending_jobs(&self) -> Result<Vec<Job>> {
        let jobs = self.store.list::<Job>()?;
        Ok(jobs
            .into_iter()
            .filter(|j| {
                j.status == JobStatus::Pending
                    && j.should_execute(&self.node_id)
            })
            .collect())
    }

    // === Gossip Operations ===

    /// Broadcast job progress.
    pub fn broadcast_progress(
        &mut self,
        job_id: &ContentId,
        progress: f32,
        message: Option<String>,
    ) -> ContentId {
        let update = JobProgressUpdate {
            job_id: job_id.clone(),
            progress,
            message,
        };

        let msg = update.to_gossip(60, self.node_id);
        self.gossip.broadcast(msg)
    }

    /// Receive a gossip message.
    pub fn receive_gossip(&mut self, msg: GossipMessage) -> std::result::Result<bool, citadel_gossip::GossipError> {
        self.gossip.receive(msg)
    }

    /// Pop next incoming gossip message.
    pub fn pop_gossip(&mut self) -> Option<GossipMessage> {
        self.gossip.pop_inbox()
    }

    /// Drain outgoing gossip messages.
    pub fn drain_gossip(&mut self) -> Vec<GossipMessage> {
        self.gossip.drain_outbox()
    }

    // === Ping Operations ===

    /// Receive a ping from a peer.
    pub fn receive_ping(&mut self, ping: &Ping) {
        self.peers.receive_ping(ping);
    }

    /// Create a ping message for broadcast.
    pub async fn create_ping(&mut self) -> Ping {
        let load = *self.load.read().await;
        self.peers.create_ping(self.capabilities.clone(), load)
    }

    /// Check if we should send a ping.
    pub fn should_ping(&self) -> bool {
        self.peers.should_ping()
    }

    /// Get peer count.
    pub fn peer_count(&self) -> usize {
        self.peers.peer_count()
    }

    /// Get alive peer count.
    pub fn alive_peer_count(&self) -> usize {
        self.peers.alive_count()
    }

    /// Find best peer for a capability.
    pub fn find_peer_for(&self, cap: Capability) -> Option<U256> {
        self.peers.best_peer_for(cap).map(|p| p.node_id)
    }

    /// Garbage collect stale peers.
    pub fn gc_peers(&mut self) {
        self.peers.gc();
    }

    // === Internal ===

    async fn update_load(&self, running_jobs: usize) {
        // Simple load calculation: running_jobs / max_concurrent
        const MAX_CONCURRENT: usize = 4;
        let load = (running_jobs as f32 / MAX_CONCURRENT as f32).min(1.0);
        *self.load.write().await = load;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_config() -> NodeConfig {
        let dir = tempdir().unwrap();
        NodeConfig {
            data_dir: dir.into_path(),
            capabilities: vec![Capability::Audit, Capability::Transcode],
            node_id_seed: Some(12345),
        }
    }

    #[test]
    fn test_node_creation() {
        let config = test_config();
        let node = LibrarianNode::new(config).unwrap();

        assert_eq!(node.capabilities().len(), 2);
        assert_eq!(node.peer_count(), 0);
    }

    #[test]
    fn test_job_lifecycle() {
        let config = test_config();
        let mut node = LibrarianNode::new(config).unwrap();

        // Create job
        let job = node.create_job(JobType::Audit, JobTarget::All).unwrap();
        assert_eq!(job.status, JobStatus::Pending);

        // Claim job
        let claimed = node.claim_job(&job.id).unwrap();
        assert!(claimed.claims.contains(node.node_id()));

        // Get job
        let fetched = node.get_job(&job.id).unwrap().unwrap();
        assert_eq!(fetched.id, job.id);
    }

    #[tokio::test]
    async fn test_job_execution() {
        let config = test_config();
        let mut node = LibrarianNode::new(config).unwrap();

        // Create and claim job
        let job = node.create_job(JobType::Audit, JobTarget::All).unwrap();
        node.claim_job(&job.id).unwrap();

        // Start job
        let started = node.start_job(&job.id).await.unwrap();
        assert_eq!(started.status, JobStatus::Running);

        // Check load increased
        assert!(node.load().await > 0.0);

        // Complete job
        let completed = node.complete_job(&job.id, JobResult::Audit {
            total_releases: 5,
            releases_with_issues: 0,
            audits: vec![],
            issue_counts: std::collections::HashMap::new(),
            source_quality: Some("flac".to_string()),
            missing_formats: Some(vec![]),
        }).await.unwrap();
        assert_eq!(completed.status, JobStatus::Completed);

        // Check load decreased
        assert_eq!(node.load().await, 0.0);
    }

    #[test]
    fn test_list_jobs() {
        let config = test_config();
        let mut node = LibrarianNode::new(config).unwrap();

        // Create multiple jobs
        node.create_job(JobType::Audit, JobTarget::All).unwrap();
        node.create_job(JobType::Transcode, JobTarget::All).unwrap();
        node.create_job(JobType::Migrate, JobTarget::All).unwrap();

        let jobs = node.list_jobs().unwrap();
        assert_eq!(jobs.len(), 3);
    }

    #[test]
    fn test_gossip() {
        let config = test_config();
        let mut node = LibrarianNode::new(config).unwrap();

        // Create job for progress broadcast
        let job = node.create_job(JobType::Audit, JobTarget::All).unwrap();

        // Broadcast progress
        let msg_id = node.broadcast_progress(&job.id, 0.5, Some("Halfway".to_string()));
        assert!(!msg_id.as_bytes().iter().all(|&b| b == 0));

        // Check outbox
        let outgoing = node.drain_gossip();
        assert_eq!(outgoing.len(), 1);
    }
}
