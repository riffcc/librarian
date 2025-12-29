//! Import management handlers.
//!
//! Imports are staged operations that download from Archive.org,
//! allow tag/filename editing, then upload to Archivist.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::api::ApiState;

/// Import status.
#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ImportStatus {
    /// User is browsing/selecting files.
    Browsing,
    /// Ready to start import.
    Pending,
    /// Downloading from source.
    Downloading,
    /// Uploading to Archivist.
    Uploading,
    /// Generating quality ladder.
    Transcoding,
    /// Import complete.
    Complete,
    /// Import failed.
    Failed,
}

/// Import file entry.
#[derive(Serialize, Deserialize, Clone)]
pub struct ImportFile {
    /// Original filename from source.
    pub original_name: String,

    /// User-edited filename (optional).
    pub edited_name: Option<String>,

    /// File format.
    pub format: Option<String>,

    /// File size in bytes.
    pub size: Option<u64>,

    /// Whether to include in import.
    pub selected: bool,
}

/// Import metadata.
#[derive(Serialize, Deserialize, Clone)]
pub struct ImportMetadata {
    /// Artist name.
    pub artist: Option<String>,

    /// Album/release name.
    pub album: Option<String>,

    /// Year.
    pub year: Option<u32>,

    /// Tags.
    pub tags: Vec<String>,
}

/// Import response.
#[derive(Serialize)]
pub struct ImportResponse {
    /// Import ID.
    pub id: String,

    /// Source identifier (e.g., Archive.org item ID).
    pub source: String,

    /// Current status.
    pub status: ImportStatus,

    /// Files to import.
    pub files: Vec<ImportFile>,

    /// Metadata.
    pub metadata: ImportMetadata,

    /// Auto-approve after import.
    pub auto_approve: bool,

    /// Creation timestamp.
    pub created_at: u64,
}

/// List all imports.
pub async fn list_imports(
    State(_state): State<Arc<ApiState>>,
) -> Json<Vec<ImportResponse>> {
    // TODO: Implement import persistence
    Json(vec![])
}

/// Get a specific import.
pub async fn get_import(
    State(_state): State<Arc<ApiState>>,
    Path(import_id): Path<String>,
) -> Result<Json<ImportResponse>, (StatusCode, String)> {
    // TODO: Implement import persistence
    Err((StatusCode::NOT_FOUND, format!("Import not found: {}", import_id)))
}

/// Create import request.
#[derive(Deserialize)]
pub struct CreateImportRequest {
    /// Source identifier (Archive.org item ID).
    pub source: String,

    /// Files to include.
    pub files: Vec<ImportFile>,

    /// Initial metadata.
    pub metadata: ImportMetadata,

    /// Auto-approve after import.
    #[serde(default)]
    pub auto_approve: bool,
}

/// Create a new import.
pub async fn create_import(
    State(_state): State<Arc<ApiState>>,
    Json(request): Json<CreateImportRequest>,
) -> Result<Json<ImportResponse>, (StatusCode, String)> {
    // TODO: Implement import creation
    // For now, return a stub response
    let import_id = format!("import_{}", chrono::Utc::now().timestamp());

    Ok(Json(ImportResponse {
        id: import_id,
        source: request.source,
        status: ImportStatus::Pending,
        files: request.files,
        metadata: request.metadata,
        auto_approve: request.auto_approve,
        created_at: chrono::Utc::now().timestamp() as u64,
    }))
}
