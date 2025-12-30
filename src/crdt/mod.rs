//! CRDT types for Librarian.
//!
//! All state in Librarian is CRDT-based, enabling geodistributed coordination
//! without external databases or coordination services.

pub mod job;

pub use job::{
    AudioQuality, AuditIssue, Job, JobOp, JobResult, JobStatus, JobTarget, JobType,
    MissingField, PartialMetadata, ProvidedMetadata, ReleaseAudit, UploadAuth,
};
