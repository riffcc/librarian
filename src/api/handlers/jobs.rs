//! Job CRDT handlers.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use citadel_crdt::ContentId;
use serde::{Deserialize, Serialize};

use crate::api::ApiState;
use crate::crdt::{Job, JobResult, JobTarget, JobType, ProvidedMetadata};

/// Job response (serializable).
#[derive(Serialize)]
pub struct JobResponse {
    /// Job ID (hex-encoded).
    pub id: String,

    /// Job type.
    pub job_type: String,

    /// Job target.
    pub target: String,

    /// Current status.
    pub status: String,

    /// Creation timestamp.
    pub created_at: u64,

    /// Number of claims.
    pub claim_count: usize,

    /// Assigned executor (if any).
    pub executor: Option<String>,

    /// Result (if completed).
    pub result: Option<serde_json::Value>,

    /// Retry count (0 = original attempt).
    pub retry_count: u64,

    /// Whether the job has been archived.
    pub archived: bool,
}

impl From<&Job> for JobResponse {
    fn from(job: &Job) -> Self {
        let job_type = match &job.job_type {
            JobType::Audit => "audit",
            JobType::Transcode => "transcode",
            JobType::Migrate => "migrate",
            JobType::Import => "import",
            JobType::SourceImport => "source_import",
            JobType::Analyze => "analyze",
        };

        let target = match &job.target {
            JobTarget::Release(id) => format!("release:{}", id),
            JobTarget::Category(cat) => format!("category:{}", cat),
            JobTarget::All => "all".to_string(),
            JobTarget::ArchiveOrgItem(id) => format!("archive.org:{}", id),
            JobTarget::Source { source, .. } => format!("source:{}", source),
        };

        let status = format!("{:?}", job.status);

        let executor = job
            .executor()
            .map(|id| hex::encode(id.to_be_bytes())[..16].to_string());

        let result = job.result.as_ref().map(|r| {
            serde_json::to_value(r).unwrap_or(serde_json::Value::Null)
        });

        JobResponse {
            id: hex::encode(job.id.as_bytes()),
            job_type: job_type.to_string(),
            target,
            status,
            created_at: job.created_at,
            claim_count: job.claims.len(),
            executor,
            result,
            retry_count: job.retry_count,
            archived: job.archived,
        }
    }
}

/// List all jobs.
pub async fn list_jobs(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<Vec<JobResponse>>, (StatusCode, String)> {
    let node = state.node.read().await;

    let jobs = node
        .list_jobs()
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to list jobs");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    let responses: Vec<JobResponse> = jobs.iter().map(JobResponse::from).collect();

    Ok(Json(responses))
}

/// Get a specific job.
pub async fn get_job(
    State(state): State<Arc<ApiState>>,
    Path(job_id): Path<String>,
) -> Result<Json<JobResponse>, (StatusCode, String)> {
    let node = state.node.read().await;

    // Parse hex job ID
    let id_bytes = hex::decode(&job_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid job ID format".to_string()))?;

    if id_bytes.len() != 32 {
        return Err((StatusCode::BAD_REQUEST, "Job ID must be 32 bytes".to_string()));
    }

    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&id_bytes);
    let content_id = ContentId::from_bytes(bytes);

    let job = node
        .get_job(&content_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, "Job not found".to_string()))?;

    Ok(Json(JobResponse::from(&job)))
}

/// Create job request.
#[derive(Deserialize)]
pub struct CreateJobRequest {
    /// Job type: "audit", "transcode", "migrate", "import".
    pub job_type: String,

    /// Target: "all", "release:<id>", "category:<name>", "archive.org:<id>".
    pub target: String,

    /// Pre-authorized public key for Archivist uploads.
    /// Format: "ed25519p/{hex}" as used by Citadel/Flagship.
    pub pubkey: Option<String>,

    /// Existing release ID to update after job completion.
    /// Used when fixing issues on existing releases.
    pub existing_release_id: Option<String>,
}

/// Start job request (optional pubkey for authorization).
#[derive(Deserialize, Default)]
pub struct StartJobRequest {
    /// Pre-authorized public key for Archivist uploads.
    /// If provided, updates the job's auth before starting.
    pub pubkey: Option<String>,
}

/// Start/retry a pending job.
pub async fn start_job(
    State(state): State<Arc<ApiState>>,
    Path(job_id): Path<String>,
    body: Option<Json<StartJobRequest>>,
) -> Result<Json<JobResponse>, (StatusCode, String)> {
    // Parse hex job ID
    let id_bytes = hex::decode(&job_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid job ID format".to_string()))?;

    if id_bytes.len() != 32 {
        return Err((StatusCode::BAD_REQUEST, "Job ID must be 32 bytes".to_string()));
    }

    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&id_bytes);
    let content_id = ContentId::from_bytes(bytes);

    // Get job, optionally update auth pubkey
    let job = {
        let request = body.map(|b| b.0).unwrap_or_default();

        if let Some(pubkey) = request.pubkey {
            // Update auth pubkey before starting
            let mut node = state.node.write().await;
            node.update_job_auth(&content_id, pubkey)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        } else {
            let node = state.node.read().await;
            node.get_job(&content_id)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
                .ok_or((StatusCode::NOT_FOUND, "Job not found".to_string()))?
        }
    };

    // Notify worker to execute (event-driven)
    state.notify_job(content_id).await;

    Ok(Json(JobResponse::from(&job)))
}

/// Cancel/stop a job (marks as failed).
pub async fn stop_job(
    State(state): State<Arc<ApiState>>,
    Path(job_id): Path<String>,
) -> Result<Json<JobResponse>, (StatusCode, String)> {
    use crate::crdt::JobResult;

    // Parse hex job ID
    let id_bytes = hex::decode(&job_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid job ID format".to_string()))?;

    if id_bytes.len() != 32 {
        return Err((StatusCode::BAD_REQUEST, "Job ID must be 32 bytes".to_string()));
    }

    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&id_bytes);
    let content_id = ContentId::from_bytes(bytes);

    // Complete job with cancellation error
    let job = {
        let mut node = state.node.write().await;
        node.complete_job(&content_id, JobResult::Error("Cancelled by user".to_string()))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        node.get_job(&content_id)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .ok_or((StatusCode::NOT_FOUND, "Job not found".to_string()))?
    };

    Ok(Json(JobResponse::from(&job)))
}

/// Retry a failed or completed job.
pub async fn retry_job(
    State(state): State<Arc<ApiState>>,
    Path(job_id): Path<String>,
) -> Result<Json<JobResponse>, (StatusCode, String)> {
    // Parse hex job ID
    let id_bytes = hex::decode(&job_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid job ID format".to_string()))?;

    if id_bytes.len() != 32 {
        return Err((StatusCode::BAD_REQUEST, "Job ID must be 32 bytes".to_string()));
    }

    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&id_bytes);
    let content_id = ContentId::from_bytes(bytes);

    // Retry the job and claim it for this node
    let job = {
        let mut node = state.node.write().await;
        node.retry_job(&content_id)
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        // Claim the job so this node becomes the executor
        node.claim_job(&content_id)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    };

    // Notify worker to pick up the retried job
    state.notify_job(content_id).await;

    Ok(Json(JobResponse::from(&job)))
}

/// Create a new job.
pub async fn create_job(
    State(state): State<Arc<ApiState>>,
    Json(request): Json<CreateJobRequest>,
) -> Result<Json<JobResponse>, (StatusCode, String)> {
    let job_type = match request.job_type.as_str() {
        "audit" => JobType::Audit,
        "transcode" => JobType::Transcode,
        "migrate" => JobType::Migrate,
        "import" => JobType::Import,
        "source_import" => JobType::SourceImport,
        "analyze" => JobType::Analyze,
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("Invalid job type: {}", request.job_type),
            ))
        }
    };

    let target = if request.target == "all" {
        JobTarget::All
    } else if let Some(id) = request.target.strip_prefix("release:") {
        JobTarget::Release(id.to_string())
    } else if let Some(cat) = request.target.strip_prefix("category:") {
        JobTarget::Category(cat.to_string())
    } else if let Some(id) = request.target.strip_prefix("archive.org:") {
        JobTarget::ArchiveOrgItem(id.to_string())
    } else if let Some(source) = request.target.strip_prefix("source:") {
        JobTarget::Source {
            source: source.to_string(),
            gateway: None,
            existing_release_id: request.existing_release_id.clone(),
        }
    } else {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Invalid target format: {}", request.target),
        ));
    };

    let job = {
        let mut node = state.node.write().await;
        node.create_job_with_auth(job_type, target, request.pubkey)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    };

    // Notify worker immediately (event-driven, no polling)
    state.notify_job(job.id.clone()).await;

    Ok(Json(JobResponse::from(&job)))
}

/// Archive a job (hide from main queue view).
pub async fn archive_job(
    State(state): State<Arc<ApiState>>,
    Path(job_id): Path<String>,
) -> Result<Json<JobResponse>, (StatusCode, String)> {
    // Parse hex job ID
    let id_bytes = hex::decode(&job_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid job ID format".to_string()))?;

    if id_bytes.len() != 32 {
        return Err((StatusCode::BAD_REQUEST, "Job ID must be 32 bytes".to_string()));
    }

    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&id_bytes);
    let content_id = ContentId::from_bytes(bytes);

    let job = {
        let mut node = state.node.write().await;
        node.archive_job(&content_id)
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
    };

    Ok(Json(JobResponse::from(&job)))
}

/// Query parameters for delete job.
#[derive(Deserialize, Default)]
pub struct DeleteJobQuery {
    /// Force delete even if not archived.
    #[serde(default)]
    pub force: bool,
}

/// Delete a specific job.
/// By default, only archived jobs can be deleted.
/// Use ?force=true to delete any job (for bulk cleanup).
pub async fn delete_job(
    State(state): State<Arc<ApiState>>,
    Path(job_id): Path<String>,
    Query(query): Query<DeleteJobQuery>,
) -> Result<StatusCode, (StatusCode, String)> {
    // Parse hex job ID
    let id_bytes = hex::decode(&job_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid job ID format".to_string()))?;

    if id_bytes.len() != 32 {
        return Err((StatusCode::BAD_REQUEST, "Job ID must be 32 bytes".to_string()));
    }

    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&id_bytes);
    let content_id = ContentId::from_bytes(bytes);

    {
        let mut node = state.node.write().await;
        node.delete_job(&content_id, query.force)
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    }

    Ok(StatusCode::NO_CONTENT)
}

/// Response for clear archived jobs.
#[derive(Serialize)]
pub struct ClearArchivedResponse {
    /// Number of jobs deleted.
    pub deleted: usize,
}

/// Delete all archived jobs.
pub async fn clear_archived_jobs(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<ClearArchivedResponse>, (StatusCode, String)> {
    let deleted = {
        let mut node = state.node.write().await;
        node.delete_all_archived_jobs()
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    };

    Ok(Json(ClearArchivedResponse { deleted }))
}

/// Request to provide metadata for a NeedsInput job.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProvideMetadataRequest {
    /// Artist name (required)
    pub artist: String,
    /// Album title (required)
    pub album: String,
    /// Release year (optional)
    pub year: Option<u32>,
    /// Track titles for renaming (optional)
    #[serde(default)]
    pub track_titles: Vec<String>,
    /// Cover art CID (optional)
    pub cover_cid: Option<String>,
}

impl From<ProvideMetadataRequest> for ProvidedMetadata {
    fn from(req: ProvideMetadataRequest) -> Self {
        ProvidedMetadata {
            artist: req.artist,
            album: req.album,
            year: req.year,
            track_titles: req.track_titles,
            cover_cid: req.cover_cid,
        }
    }
}

/// Provide metadata for a job that returned NeedsInput.
/// This stores the metadata and restarts the job to complete the import.
pub async fn provide_metadata(
    State(state): State<Arc<ApiState>>,
    Path(job_id): Path<String>,
    Json(request): Json<ProvideMetadataRequest>,
) -> Result<Json<JobResponse>, (StatusCode, String)> {
    // Parse hex job ID
    let id_bytes = hex::decode(&job_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid job ID format".to_string()))?;

    if id_bytes.len() != 32 {
        return Err((StatusCode::BAD_REQUEST, "Job ID must be 32 bytes".to_string()));
    }

    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&id_bytes);
    let content_id = ContentId::from_bytes(bytes);

    // Get the job and verify it's in NeedsInput state
    let job = {
        let node = state.node.read().await;
        node.get_job(&content_id)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .ok_or((StatusCode::NOT_FOUND, "Job not found".to_string()))?
    };

    // Check if job result is NeedsInput
    match &job.result {
        Some(JobResult::NeedsInput { .. }) => {
            // Good - this is what we expect
        }
        Some(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                "Job is not waiting for input".to_string(),
            ));
        }
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                "Job has no result - may still be running".to_string(),
            ));
        }
    }

    // Store the provided metadata and restart the job
    let job = {
        let mut node = state.node.write().await;
        node.provide_job_metadata(&content_id, request.into())
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    };

    // Notify worker to execute the job with the new metadata
    state.notify_job(content_id).await;

    Ok(Json(JobResponse::from(&job)))
}
