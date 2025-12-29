//! Job CRDT for Librarian.
//!
//! Jobs are Bilateral CRDT documents - they can be created and modified
//! on any Librarian instance with zero latency, and will converge to
//! the same state across all instances via SPORE sync.
//!
//! Key properties (proven in Lean):
//! - Merge is total: cannot fail
//! - Merge is deterministic: same operations -> same state
//! - Claims are lossless: all claims preserved in union
//! - Executor selection is pure: deterministic function of claims

use citadel_crdt::{AssociativeMerge, CommutativeMerge, ContentId, IdempotentMerge, TotalMerge};
use citadel_docs::Document;
use citadel_spore::U256;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Type of job to execute.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobType {
    /// Audit a release for quality ladder compliance.
    Audit,

    /// Transcode lossless source to Opus ladder.
    Transcode,

    /// Migrate content from IPFS to Archivist.
    Migrate,

    /// Import from Archive.org.
    Import,

    /// Import from URL or CID (IPFS/Archivist).
    SourceImport,
}

/// Job status as a lattice - merge takes "most progressed".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum JobStatus {
    /// Not yet started.
    Pending = 0,

    /// Currently executing.
    Running = 1,

    /// Completed successfully.
    Completed = 2,

    /// Failed.
    Failed = 3,
}

impl Default for JobStatus {
    fn default() -> Self {
        Self::Pending
    }
}

/// Target of the job operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobTarget {
    /// Specific release by ID.
    Release(String),

    /// All releases in a category.
    Category(String),

    /// All releases.
    All,

    /// Archive.org item identifier.
    ArchiveOrgItem(String),

    /// URL or CID source for import.
    /// Contains the source URL/CID and optional gateway override.
    Source {
        /// The source URL or CID (http://, https://, Qm..., bafy..., zD..., zE...).
        source: String,
        /// Optional gateway URL for CID resolution.
        gateway: Option<String>,
        /// Optional release ID to update after import.
        existing_release_id: Option<String>,
    },
}

/// Result of a completed job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobResult {
    /// Audit completed - contains report.
    Audit {
        missing_formats: Vec<String>,
        source_quality: String,
    },

    /// Transcode completed - contains output CIDs.
    Transcode {
        outputs: Vec<TranscodeOutput>,
    },

    /// Migration completed.
    Migrate {
        old_cid: String,
        new_cid: String,
        size: u64,
    },

    /// Import completed.
    Import {
        files_imported: usize,
    },

    /// Source import completed - URL/CID imported to Archivist.
    SourceImport {
        /// Original source URL or CID.
        source: String,
        /// New Archivist CID.
        new_cid: String,
        /// Size in bytes.
        size: u64,
        /// Content type if detected.
        content_type: Option<String>,
    },

    /// Job failed with error message.
    Error(String),
}

/// Output from a transcode operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscodeOutput {
    /// Quality level (e.g., "opus_128").
    pub quality: String,

    /// Content ID of the transcoded file.
    pub cid: ContentId,

    /// Size in bytes.
    pub size: usize,
}

/// Job operations - applied to the operation log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobOp {
    /// Create a new job.
    Create(Job),

    /// Claim a job for execution.
    Claim {
        job_id: ContentId,
        node_id: U256,
    },

    /// Mark job as running.
    Start {
        job_id: ContentId,
    },

    /// Complete a job with result.
    Complete {
        job_id: ContentId,
        result: JobResult,
    },
}

/// A job document with CRDT semantics.
///
/// Jobs preserve ALL claims (lossless union) and deterministically
/// select an executor based on the claims set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// Unique job identifier (content-addressed).
    pub id: ContentId,

    /// Type of job to execute.
    pub job_type: JobType,

    /// Target of the operation.
    pub target: JobTarget,

    /// Current status (lattice join = max).
    pub status: JobStatus,

    /// When the job was created (Unix epoch seconds).
    pub created_at: u64,

    /// All nodes that have claimed this job (lossless union).
    pub claims: BTreeSet<U256>,

    /// Result if completed.
    pub result: Option<JobResult>,

    /// Pre-authorized public key for Archivist uploads.
    /// Set at job creation time by the user who initiated the import.
    /// Format: "ed25519p/{hex}" as used by Citadel/Flagship.
    #[serde(default)]
    pub auth_pubkey: Option<String>,

    /// Signature over "{timestamp}:UPLOAD" for Archivist authorization.
    /// Hex-encoded ed25519 signature created by Flagship at job creation.
    #[serde(default)]
    pub auth_signature: Option<String>,

    /// Unix timestamp (seconds) when the signature was created.
    /// Must be within 24 hours of upload execution.
    #[serde(default)]
    pub auth_timestamp: Option<u64>,
}

/// Upload authorization credentials for Archivist.
#[derive(Debug, Clone, Default)]
pub struct UploadAuth {
    /// Public key in "ed25519p/{hex}" format.
    pub pubkey: Option<String>,
    /// Hex-encoded signature over "{timestamp}:UPLOAD".
    pub signature: Option<String>,
    /// Unix timestamp when signature was created.
    pub timestamp: Option<u64>,
}

impl Job {
    /// Create a new pending job.
    pub fn new(job_type: JobType, target: JobTarget) -> Self {
        Self::new_with_auth(job_type, target, UploadAuth::default())
    }

    /// Create a new pending job with pre-authorized credentials for uploads.
    pub fn new_with_auth(job_type: JobType, target: JobTarget, auth: UploadAuth) -> Self {
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Generate content ID from job contents
        let content = (job_type.clone(), target.clone(), created_at);
        let bytes = bincode::serialize(&content).expect("serialization cannot fail");
        let id = ContentId::hash(&bytes);

        Self {
            id,
            job_type,
            target,
            status: JobStatus::Pending,
            created_at,
            claims: BTreeSet::new(),
            result: None,
            auth_pubkey: auth.pubkey,
            auth_signature: auth.signature,
            auth_timestamp: auth.timestamp,
        }
    }

    /// Get upload authorization credentials.
    pub fn upload_auth(&self) -> UploadAuth {
        UploadAuth {
            pubkey: self.auth_pubkey.clone(),
            signature: self.auth_signature.clone(),
            timestamp: self.auth_timestamp,
        }
    }

    /// Deterministic executor selection - pure function of claims set.
    ///
    /// All nodes compute the same result given the same claims.
    /// This is NOT "winning" or "losing" - just the output of a pure function.
    pub fn executor(&self) -> Option<&U256> {
        // Lexicographically smallest NodeId executes.
        // Deterministic: same claims -> same executor.
        self.claims.iter().next()
    }

    /// Check if this node should execute the job.
    pub fn should_execute(&self, my_id: &U256) -> bool {
        self.executor() == Some(my_id)
    }

    /// Claim this job for execution.
    pub fn claim(&mut self, node_id: U256) {
        self.claims.insert(node_id);
    }

    /// Start execution.
    pub fn start(&mut self) {
        if self.status == JobStatus::Pending {
            self.status = JobStatus::Running;
        }
    }

    /// Complete with result.
    pub fn complete(&mut self, result: JobResult) {
        self.status = match &result {
            JobResult::Error(_) => JobStatus::Failed,
            _ => JobStatus::Completed,
        };
        self.result = Some(result);
    }
}

impl TotalMerge for Job {
    /// Total merge - cannot fail, preserves all information.
    ///
    /// Properties (proven in Lean):
    /// - Lossless: All claims are preserved in the union
    /// - Total: Merge always succeeds, returns valid Job
    /// - Deterministic: Same inputs -> same output
    /// - Convergent: Same operations applied -> same state
    fn merge(&self, other: &Self) -> Self {
        debug_assert_eq!(self.id, other.id, "Cannot merge jobs with different IDs");

        // Status: lattice join (max)
        let status = self.status.max(other.status);

        // Claims: union (lossless - no claim is lost)
        let claims: BTreeSet<U256> = self.claims.union(&other.claims).cloned().collect();

        // Result: first completed result (idempotent)
        let result = self.result.clone().or_else(|| other.result.clone());

        // Auth fields: first non-None wins (set at creation, never changes)
        let auth_pubkey = self.auth_pubkey.clone().or_else(|| other.auth_pubkey.clone());
        let auth_signature = self.auth_signature.clone().or_else(|| other.auth_signature.clone());
        let auth_timestamp = self.auth_timestamp.or(other.auth_timestamp);

        Job {
            id: self.id.clone(),
            job_type: self.job_type.clone(),
            target: self.target.clone(),
            status,
            created_at: self.created_at.min(other.created_at),
            claims,
            result,
            auth_pubkey,
            auth_signature,
            auth_timestamp,
        }
    }
}

// CRDT marker traits - Job is a proper Bilateral CRDT
impl CommutativeMerge for Job {}
impl AssociativeMerge for Job {}
impl IdempotentMerge for Job {}

impl Document for Job {
    const TYPE_PREFIX: &'static str = "job";

    fn content_id(&self) -> ContentId {
        self.id.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node_id(n: u64) -> U256 {
        U256::from_u64(n)
    }

    #[test]
    fn test_job_creation() {
        let job = Job::new(JobType::Audit, JobTarget::All);

        assert_eq!(job.status, JobStatus::Pending);
        assert!(job.claims.is_empty());
        assert!(job.result.is_none());
    }

    #[test]
    fn test_job_claim() {
        let mut job = Job::new(JobType::Audit, JobTarget::All);

        job.claim(make_node_id(5));
        job.claim(make_node_id(3));
        job.claim(make_node_id(7));

        // Executor is lexicographically smallest
        assert_eq!(job.executor(), Some(&make_node_id(3)));
        assert!(job.should_execute(&make_node_id(3)));
        assert!(!job.should_execute(&make_node_id(5)));
    }

    #[test]
    fn test_job_merge_claims() {
        let mut job1 = Job::new(JobType::Audit, JobTarget::All);
        let mut job2 = job1.clone();

        job1.claim(make_node_id(1));
        job1.claim(make_node_id(3));

        job2.claim(make_node_id(2));
        job2.claim(make_node_id(3));

        let merged = job1.merge(&job2);

        // Union of claims
        assert_eq!(merged.claims.len(), 3);
        assert!(merged.claims.contains(&make_node_id(1)));
        assert!(merged.claims.contains(&make_node_id(2)));
        assert!(merged.claims.contains(&make_node_id(3)));
    }

    #[test]
    fn test_job_merge_status() {
        let mut job1 = Job::new(JobType::Audit, JobTarget::All);
        let mut job2 = job1.clone();

        job1.status = JobStatus::Running;
        job2.status = JobStatus::Completed;

        let merged = job1.merge(&job2);

        // Max status wins
        assert_eq!(merged.status, JobStatus::Completed);
    }

    #[test]
    fn test_job_merge_result() {
        let mut job1 = Job::new(JobType::Audit, JobTarget::All);
        let mut job2 = job1.clone();

        job1.complete(JobResult::Audit {
            missing_formats: vec!["opus_128".to_string()],
            source_quality: "flac".to_string(),
        });

        let merged = job1.merge(&job2);

        // Result preserved
        assert!(merged.result.is_some());
    }

    #[test]
    fn test_executor_deterministic() {
        let mut job1 = Job::new(JobType::Audit, JobTarget::All);
        let mut job2 = job1.clone();

        // Different order of claims
        job1.claim(make_node_id(5));
        job1.claim(make_node_id(3));
        job1.claim(make_node_id(7));

        job2.claim(make_node_id(7));
        job2.claim(make_node_id(5));
        job2.claim(make_node_id(3));

        // Same executor regardless of claim order
        assert_eq!(job1.executor(), job2.executor());
        assert_eq!(job1.executor(), Some(&make_node_id(3)));
    }
}
