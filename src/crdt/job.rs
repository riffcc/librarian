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

    /// Analyze content to extract metadata (audio quality, track info).
    /// Does NOT re-upload - just fetches, analyzes, and updates Citadel Lens.
    Analyze,
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

/// Audio quality metadata matching Flagship's expected format.
/// Used to display codec badges (FLAC, MP3 320, Opus, etc.)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AudioQuality {
    /// Audio format: flac, mp3, aac, opus, vorbis, wav, other
    pub format: String,
    /// Bitrate in kbps (e.g., 320, 256, 192) - for lossy formats
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bitrate: Option<u32>,
    /// Sample rate in Hz (e.g., 44100, 48000, 96000)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<u32>,
    /// Bit depth (e.g., 16, 24, 32) - for lossless formats
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bit_depth: Option<u32>,
    /// Codec name if known (e.g., "LAME V0", "Opus", "FLAC")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
}

impl AudioQuality {
    /// Create FLAC quality (16-bit CD quality by default)
    pub fn flac() -> Self {
        Self {
            format: "flac".to_string(),
            bitrate: None,
            sample_rate: Some(44100),
            bit_depth: Some(16),
            codec: Some("FLAC".to_string()),
        }
    }

    /// Create 24-bit FLAC quality
    pub fn flac_24() -> Self {
        Self {
            format: "flac".to_string(),
            bitrate: None,
            sample_rate: Some(44100),
            bit_depth: Some(24),
            codec: Some("FLAC".to_string()),
        }
    }

    /// Create MP3 quality with specified bitrate
    pub fn mp3(bitrate: u32) -> Self {
        Self {
            format: "mp3".to_string(),
            bitrate: Some(bitrate),
            sample_rate: None,
            bit_depth: None,
            codec: Some("MP3".to_string()),
        }
    }

    /// Create MP3 VBR quality
    pub fn mp3_vbr() -> Self {
        Self {
            format: "mp3".to_string(),
            bitrate: Some(245), // V0 average
            sample_rate: None,
            bit_depth: None,
            codec: Some("LAME VBR".to_string()),
        }
    }

    /// Create Ogg Vorbis quality
    pub fn vorbis() -> Self {
        Self {
            format: "vorbis".to_string(),
            bitrate: None,
            sample_rate: None,
            bit_depth: None,
            codec: Some("Vorbis".to_string()),
        }
    }

    /// Create Opus quality with optional bitrate
    pub fn opus(bitrate: Option<u32>) -> Self {
        Self {
            format: "opus".to_string(),
            bitrate,
            sample_rate: None,
            bit_depth: None,
            codec: Some("Opus".to_string()),
        }
    }

    /// Create AAC quality
    pub fn aac() -> Self {
        Self {
            format: "aac".to_string(),
            bitrate: None,
            sample_rate: None,
            bit_depth: None,
            codec: Some("AAC".to_string()),
        }
    }

    /// Create WAV quality (lossless PCM)
    pub fn wav() -> Self {
        Self {
            format: "wav".to_string(),
            bitrate: None,
            sample_rate: Some(44100),
            bit_depth: Some(16),
            codec: Some("PCM".to_string()),
        }
    }
}

/// Types of quality issues detected during audit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AuditIssue {
    /// No audio quality metadata (format/codec/bitrate unknown)
    MissingAudioQuality,
    /// No license information
    MissingLicense,
    /// No source URL (and not marked as Unknown or Self)
    MissingSource,
    /// Has lossless but missing Opus encodes for quality ladder
    MissingOpusEncodes,
    /// Could have cover art but doesn't (detected in import)
    MissingCoverArt,
    /// Has embedded track art we're not showing
    UnusedTrackArt,
    /// Missing description
    MissingDescription,
    /// Missing release year
    MissingYear,
    /// Missing credits/attribution (and not public domain)
    MissingCredits,
    /// No actual audio files in content
    NoAudioFiles,
    /// Content is on IPFS but not on Archivist
    NotOnArchivist,
    /// Invalid or missing content CID
    InvalidContentCid,
    /// Has files but no track metadata
    MissingTrackMetadata,
    /// Archive.org source that could be re-fetched for better quality
    CanRefetchFromArchive { identifier: String },
}

/// Audit result for a single release.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseAudit {
    /// Release ID
    pub release_id: String,
    /// Release title
    pub title: String,
    /// Artist name
    pub artist: Option<String>,
    /// Detected source quality tier
    pub source_quality: Option<String>,
    /// Available quality tiers
    pub available_tiers: Vec<String>,
    /// Quality tiers missing from the ideal ladder
    pub missing_tiers: Vec<String>,
    /// List of detected issues
    pub issues: Vec<AuditIssue>,
    /// Archive.org identifier if applicable (for refetch)
    pub archive_org_id: Option<String>,
    /// Content CID
    pub content_cid: Option<String>,
}

// =============================================================================
// NeedsInput Types - for interactive metadata collection
// =============================================================================

/// Fields that may be missing and need user input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingField {
    /// Album artist name
    Artist,
    /// Album title
    Album,
    /// Release year
    Year,
    /// Track titles (for renaming)
    TrackTitles,
    /// Cover art
    CoverArt,
}

/// Partial metadata detected during import - what we found before needing input.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartialMetadata {
    /// Detected artist (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artist: Option<String>,
    /// Detected album title (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub album: Option<String>,
    /// Detected year (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub year: Option<u32>,
    /// Number of audio tracks found
    pub track_count: usize,
    /// Detected audio format (e.g., "FLAC", "MP3 320")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detected_format: Option<String>,
    /// Whether embedded cover art was found
    pub has_embedded_cover: bool,
    /// Whether directory cover art was found
    pub has_directory_cover: bool,
    /// Track titles we could extract (if any)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub track_titles: Vec<Option<String>>,
}

/// User-provided metadata to complete a NeedsInput job.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProvidedMetadata {
    /// Artist name (required)
    pub artist: String,
    /// Album title (required)
    pub album: String,
    /// Release year (optional but recommended)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub year: Option<u32>,
    /// Track titles to use for renaming (optional)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub track_titles: Vec<String>,
    /// Cover art CID to use (optional - user can upload one)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cover_cid: Option<String>,
}

/// Result of a completed job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobResult {
    /// Audit completed - contains detailed report per release.
    Audit {
        /// Total releases audited
        #[serde(default)]
        total_releases: usize,
        /// Releases with at least one issue
        #[serde(default)]
        releases_with_issues: usize,
        /// Detailed audit results per release (only those with issues)
        #[serde(default)]
        audits: Vec<ReleaseAudit>,
        /// Summary counts by issue type
        #[serde(default)]
        issue_counts: std::collections::HashMap<String, usize>,
        // Legacy fields for backwards compatibility with old audit results
        /// Legacy: source quality (single-release audit)
        #[serde(default, skip_serializing)]
        source_quality: Option<String>,
        /// Legacy: missing formats (single-release audit)
        #[serde(default, skip_serializing)]
        missing_formats: Option<Vec<String>>,
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
        /// Number of audio files imported (per tier, total across all tiers).
        files_imported: usize,
        /// Primary directory CID (highest quality tier available).
        directory_cid: String,
        /// Quality tier CIDs for the Quality Ladder.
        /// Map of tier name ("lossless", "mp3_high", "ogg", etc.) to directory CID.
        #[serde(default)]
        quality_tiers: std::collections::HashMap<String, String>,
        /// Source identifier (e.g., "archive.org:tou2016").
        source: String,
        /// Extracted album/release title.
        title: Option<String>,
        /// Extracted artist name.
        artist: Option<String>,
        /// Release date.
        date: Option<String>,
        /// Structured license info as JSON string (matches Flagship's LicenseInfo format).
        /// Contains: type, version, jurisdiction, url
        license: Option<String>,
        /// Track metadata as JSON string array (matches Flagship's trackMetadata format).
        /// Contains: [{title, artist, duration, trackNumber}, ...]
        #[serde(default)]
        track_metadata: Option<String>,
        /// Cover art CID (WebP format, 85% quality).
        #[serde(default)]
        thumbnail_cid: Option<String>,
        /// Audio quality metadata for the primary (highest quality) tier.
        /// Used for displaying codec badges (FLAC, MP3 320, Opus, etc.)
        #[serde(default)]
        audio_quality: Option<AudioQuality>,
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
        /// Cover art CID (WebP format).
        #[serde(skip_serializing_if = "Option::is_none")]
        thumbnail_cid: Option<String>,
        /// Audio quality metadata.
        #[serde(skip_serializing_if = "Option::is_none")]
        audio_quality: Option<AudioQuality>,
    },

    /// Analyze completed - metadata extracted and updated in Citadel Lens.
    Analyze {
        /// Release ID that was updated.
        release_id: String,
        /// Detected audio quality.
        audio_quality: Option<AudioQuality>,
        /// Number of tracks with metadata extracted.
        tracks_found: usize,
        /// Track metadata as JSON string.
        track_metadata: Option<String>,
    },

    /// Job failed with error message.
    Error(String),

    /// Job needs user input to continue.
    /// The job is paused and waiting for metadata to be provided via API.
    NeedsInput {
        /// Source that was being imported (URL, CID, etc.)
        source: String,
        /// Fields that are missing and need user input
        missing_fields: Vec<MissingField>,
        /// Partial metadata that was detected
        detected: PartialMetadata,
        /// Suggested Archive.org identifier if detectable from source
        #[serde(skip_serializing_if = "Option::is_none")]
        archive_org_hint: Option<String>,
        /// Temporary CID of downloaded content (for resuming)
        #[serde(skip_serializing_if = "Option::is_none")]
        temp_content_cid: Option<String>,
    },
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

    /// Retry counter - incremented when job is retried.
    /// Higher retry_count takes precedence in merge, allowing status reset.
    #[serde(default)]
    pub retry_count: u64,

    /// Whether the job has been archived (hidden from main queue view).
    /// Archived jobs can still be viewed in history but don't clutter the active queue.
    #[serde(default)]
    pub archived: bool,

    /// User-provided metadata for NeedsInput jobs.
    /// When a job returns NeedsInput, the user provides metadata via API,
    /// which is stored here and used when the job is retried.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provided_metadata: Option<ProvidedMetadata>,
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
            retry_count: 0,
            archived: false,
            provided_metadata: None,
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

    /// Retry a failed or completed job.
    /// Increments retry_count, resets status to Pending, clears claims and result.
    /// The higher retry_count ensures this state wins in CRDT merge.
    pub fn retry(&mut self) {
        self.retry_count += 1;
        self.status = JobStatus::Pending;
        self.claims.clear();
        self.result = None;
    }

    /// Check if job can be retried (failed or completed).
    pub fn can_retry(&self) -> bool {
        matches!(self.status, JobStatus::Failed | JobStatus::Completed)
    }

    /// Archive the job (hide from main queue view).
    /// Can only archive completed or failed jobs.
    pub fn archive(&mut self) -> bool {
        if self.can_retry() {
            self.archived = true;
            true
        } else {
            false
        }
    }

    /// Check if job can be archived.
    pub fn can_archive(&self) -> bool {
        self.can_retry() && !self.archived
    }
}

impl TotalMerge for Job {
    /// Total merge - cannot fail, preserves all information.
    ///
    /// Properties (proven in Lean):
    /// - Lossless: All claims are preserved in the union (within same retry epoch)
    /// - Total: Merge always succeeds, returns valid Job
    /// - Deterministic: Same inputs -> same output
    /// - Convergent: Same operations applied -> same state
    ///
    /// Retry semantics:
    /// - Higher retry_count takes precedence (allows status reset on retry)
    /// - Within same retry_count, standard lattice merge applies
    fn merge(&self, other: &Self) -> Self {
        debug_assert_eq!(self.id, other.id, "Cannot merge jobs with different IDs");

        // Retry count: max (higher retry wins)
        let retry_count = self.retry_count.max(other.retry_count);

        // If retry counts differ, take state from the one with higher retry_count
        // This allows retry() to reset status/claims/result
        let (status, claims, result) = if self.retry_count > other.retry_count {
            // Self has higher retry - use its state
            (self.status, self.claims.clone(), self.result.clone())
        } else if other.retry_count > self.retry_count {
            // Other has higher retry - use its state
            (other.status, other.claims.clone(), other.result.clone())
        } else {
            // Same retry count - use standard lattice merge
            let status = self.status.max(other.status);
            let claims: BTreeSet<U256> = self.claims.union(&other.claims).cloned().collect();
            let result = self.result.clone().or_else(|| other.result.clone());
            (status, claims, result)
        };

        // Auth fields: first non-None wins (set at creation, never changes)
        let auth_pubkey = self.auth_pubkey.clone().or_else(|| other.auth_pubkey.clone());
        let auth_signature = self.auth_signature.clone().or_else(|| other.auth_signature.clone());
        let auth_timestamp = self.auth_timestamp.or(other.auth_timestamp);

        // Archived: once archived, stays archived (monotonic OR)
        let archived = self.archived || other.archived;

        // Provided metadata: use whichever has it (prefer self, then other)
        let provided_metadata = self.provided_metadata.clone()
            .or_else(|| other.provided_metadata.clone());

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
            retry_count,
            archived,
            provided_metadata,
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
            total_releases: 10,
            releases_with_issues: 3,
            audits: vec![],
            issue_counts: std::collections::HashMap::new(),
            source_quality: Some("flac".to_string()),
            missing_formats: Some(vec!["opus_128".to_string()]),
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
