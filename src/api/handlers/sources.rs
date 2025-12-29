//! Source import handler for URL/CID imports.

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};

use crate::api::ApiState;
use crate::crdt::{JobTarget, JobType, UploadAuth};

/// Request to import content from a URL or CID.
#[derive(Debug, Deserialize)]
pub struct SourceImportRequest {
    /// The source URL or CID to import.
    /// Supported formats:
    /// - HTTP/HTTPS URLs: `http://...`, `https://...`
    /// - IPFS CIDs: `Qm...`, `bafy...`
    /// - Archivist CIDs: `zD...`, `zE...`
    pub source: String,

    /// Optional gateway URL for CID resolution.
    /// Defaults to `https://cdn.riff.cc/ipfs/` for IPFS CIDs.
    #[serde(default)]
    pub gateway: Option<String>,

    /// If set, update this release's contentCID after successful import.
    #[serde(default)]
    pub existing_release_id: Option<String>,

    /// Pre-authorized public key for Archivist uploads.
    /// Format: "ed25519p/{hex}" as used by Citadel/Flagship.
    #[serde(default)]
    pub pubkey: Option<String>,

    /// Signature over "{timestamp}:UPLOAD" for Archivist authorization.
    #[serde(default)]
    pub signature: Option<String>,

    /// Unix timestamp (seconds) when the signature was created.
    #[serde(default)]
    pub timestamp: Option<u64>,
}

/// Response from source import endpoint.
#[derive(Debug, Serialize)]
pub struct SourceImportResponse {
    /// Job ID (hex-encoded).
    pub job_id: String,
    /// Current status.
    pub status: String,
}

/// Detect the type of source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceType {
    /// HTTP/HTTPS URL.
    Url,
    /// IPFS CID (Qm..., bafy...).
    IpfsCid,
    /// Archivist CID (zD..., zE...).
    ArchivistCid,
}

/// Detect source type from string.
pub fn detect_source_type(source: &str) -> Option<SourceType> {
    let source = source.trim();

    if source.starts_with("http://") || source.starts_with("https://") {
        return Some(SourceType::Url);
    }

    // IPFS CIDv0 (Qm...) or CIDv1 (bafy...)
    if source.starts_with("Qm") && source.len() == 46 {
        return Some(SourceType::IpfsCid);
    }
    if source.starts_with("bafy") {
        return Some(SourceType::IpfsCid);
    }

    // Archivist CIDs (zD..., zE...)
    if source.starts_with("zD") || source.starts_with("zE") {
        return Some(SourceType::ArchivistCid);
    }

    None
}

/// Import content from a URL or CID.
///
/// Creates a job to fetch the content and upload to Archivist.
pub async fn import_source(
    State(state): State<Arc<ApiState>>,
    Json(request): Json<SourceImportRequest>,
) -> Result<Json<SourceImportResponse>, (StatusCode, String)> {
    // Validate source
    let source_type = detect_source_type(&request.source).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            format!(
                "Invalid source format. Expected URL (http/https), IPFS CID (Qm.../bafy...), or Archivist CID (zD.../zE...). Got: {}",
                &request.source
            ),
        )
    })?;

    tracing::info!(
        source = %request.source,
        source_type = ?source_type,
        "Creating source import job"
    );

    // Build upload auth from request
    let auth = UploadAuth {
        pubkey: request.pubkey,
        signature: request.signature,
        timestamp: request.timestamp,
    };

    // Create the job
    let job = {
        let mut node = state.node.write().await;
        node.create_job_with_auth_full(
            JobType::SourceImport,
            JobTarget::Source {
                source: request.source.clone(),
                gateway: request.gateway,
                existing_release_id: request.existing_release_id,
            },
            auth,
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    };

    let job_id_hex = hex::encode(job.id.as_bytes());

    // Notify worker immediately (event-driven)
    state.notify_job(job.id.clone()).await;

    Ok(Json(SourceImportResponse {
        job_id: job_id_hex,
        status: "pending".to_string(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_source_type() {
        // URLs
        assert_eq!(
            detect_source_type("http://example.com/file.mp3"),
            Some(SourceType::Url)
        );
        assert_eq!(
            detect_source_type("https://example.com/file.mp3"),
            Some(SourceType::Url)
        );

        // IPFS CIDv0
        assert_eq!(
            detect_source_type("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"),
            Some(SourceType::IpfsCid)
        );

        // IPFS CIDv1
        assert_eq!(
            detect_source_type("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"),
            Some(SourceType::IpfsCid)
        );

        // Archivist CIDs
        assert_eq!(
            detect_source_type("zDtest123"),
            Some(SourceType::ArchivistCid)
        );
        assert_eq!(
            detect_source_type("zEtest456"),
            Some(SourceType::ArchivistCid)
        );

        // Invalid
        assert_eq!(detect_source_type("not-a-valid-source"), None);
        assert_eq!(detect_source_type("ftp://example.com"), None);
    }
}
