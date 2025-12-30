//! Release metadata update handlers.
//!
//! Provides endpoints for updating release metadata in Citadel Lens,
//! such as license, description, year, etc.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::api::ApiState;

/// Request to update release metadata.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateMetadataRequest {
    /// License URL or object (e.g., "https://creativecommons.org/licenses/by/4.0/")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<serde_json::Value>,

    /// Description text
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Release year
    #[serde(skip_serializing_if = "Option::is_none")]
    pub year: Option<u32>,

    /// Source attribution (e.g., "archive.org:identifier")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,

    /// Credits / attribution text
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credits: Option<String>,
}

/// Response from metadata update.
#[derive(Serialize)]
pub struct UpdateMetadataResponse {
    pub success: bool,
    pub message: String,
}

/// Update release metadata in Citadel Lens.
///
/// PATCH /api/v1/releases/:id/metadata
pub async fn update_metadata(
    State(state): State<Arc<ApiState>>,
    Path(release_id): Path<String>,
    Json(request): Json<UpdateMetadataRequest>,
) -> Result<Json<UpdateMetadataResponse>, (StatusCode, String)> {
    let lens_url = state.lens_url.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, "No Lens URL configured".to_string())
    })?;

    let auth = state.auth.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, "No auth configured".to_string())
    })?;

    // Build the metadata update object
    let mut metadata = serde_json::Map::new();

    if let Some(license) = request.license {
        metadata.insert("license".to_string(), license);
    }
    if let Some(description) = request.description {
        metadata.insert("description".to_string(), serde_json::Value::String(description));
    }
    if let Some(year) = request.year {
        metadata.insert("year".to_string(), serde_json::Value::Number(year.into()));
    }
    if let Some(source) = request.source {
        metadata.insert("source".to_string(), serde_json::Value::String(source));
    }
    if let Some(credits) = request.credits {
        metadata.insert("credits".to_string(), serde_json::Value::String(credits));
    }

    if metadata.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "No metadata fields to update".to_string()));
    }

    // Wrap in metadata object for Lens API
    let mut update = serde_json::Map::new();
    update.insert("metadata".to_string(), serde_json::Value::Object(metadata));

    let update_url = format!(
        "{}/api/v1/releases/{}",
        lens_url.trim_end_matches('/'),
        release_id
    );

    info!(release_id = %release_id, updates = ?update.keys().collect::<Vec<_>>(), "Updating release metadata");

    // Sign the request body
    let body = serde_json::to_string(&serde_json::Value::Object(update))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let timestamp = crate::auth::Auth::timestamp_millis();
    let message = format!("{}:{}", timestamp, body);
    let signature = auth.sign(message.as_bytes());

    // Send PATCH request to Lens
    match state
        .http_client
        .patch(&update_url)
        .header("X-Timestamp", timestamp.to_string())
        .header("X-Signature", signature)
        .header("X-Public-Key", auth.public_key())
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            info!(release_id = %release_id, "Successfully updated release metadata");
            Ok(Json(UpdateMetadataResponse {
                success: true,
                message: "Metadata updated successfully".to_string(),
            }))
        }
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!(
                release_id = %release_id,
                status = %status,
                body = %body,
                "Failed to update release metadata"
            );
            Err((
                StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                format!("Lens API error: {}", body),
            ))
        }
        Err(e) => {
            warn!(release_id = %release_id, error = %e, "Failed to update release metadata");
            Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
        }
    }
}
