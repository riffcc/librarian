//! Release creation for Citadel Lens.
//!
//! After an import completes, creates a release in the configured
//! Citadel Lens instance with the extracted metadata.
//!
//! Metadata format matches Flagship's expected schema:
//! - `metadata.author` - artist name
//! - `metadata.trackMetadata` - JSON array of track info
//! - `thumbnailCID` - cover art CID (WebP)

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn, error};

use crate::auth::Auth;

/// Request body for creating a release in Citadel Lens.
/// Uses camelCase to match the API's expected format.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateReleaseRequest {
    /// Release title
    name: String,
    /// Category ID (defaults to "music" for Archive.org imports)
    category_id: String,
    /// Creator/artist name (shown in endless view)
    #[serde(skip_serializing_if = "Option::is_none")]
    creator: Option<String>,
    /// Content CID from Archivist
    #[serde(rename = "contentCID")]
    content_cid: String,
    /// Thumbnail/cover art CID (WebP format)
    #[serde(rename = "thumbnailCID", skip_serializing_if = "Option::is_none")]
    thumbnail_cid: Option<String>,
    /// Metadata including author, trackMetadata, license, source, etc.
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<serde_json::Value>,
    /// Status - "pending" for moderation queue
    status: String,
}

/// Response from Citadel Lens release creation (on success, returns the release).
#[derive(Debug, Deserialize)]
struct CreateReleaseSuccess {
    id: String,
}

/// Response from Citadel Lens release creation (on error).
#[derive(Debug, Deserialize)]
struct CreateReleaseError {
    error: Option<String>,
}

/// Create a release in Citadel Lens.
///
/// # Arguments
/// * `client` - HTTP client
/// * `lens_url` - Base URL of Citadel Lens API (e.g., "https://lens.riff.cc")
/// * `title` - Release title
/// * `artist` - Artist name (stored in metadata.author)
/// * `content_cid` - CID of the directory in Archivist
/// * `thumbnail_cid` - Optional cover art CID (WebP)
/// * `source` - Source identifier (e.g., "archive.org:tou2016")
/// * `license_json` - Optional license info as JSON string
/// * `date` - Optional release date
/// * `track_metadata` - Optional track metadata as JSON string
/// * `auth` - Authentication credentials for signing
///
/// # Returns
/// The release ID if successful, or an error message.
pub async fn create_release_in_lens(
    client: &Client,
    lens_url: &str,
    title: &str,
    artist: Option<&str>,
    content_cid: &str,
    thumbnail_cid: Option<&str>,
    source: &str,
    license_json: Option<&str>,
    date: Option<&str>,
    track_metadata: Option<&str>,
    auth: &Auth,
) -> Result<String, String> {
    info!(
        lens_url = %lens_url,
        title = %title,
        artist = ?artist,
        cid = %content_cid,
        thumbnail = ?thumbnail_cid,
        "Creating release in Citadel Lens"
    );

    // Build metadata object matching Flagship's expected format
    let mut metadata = serde_json::json!({
        "source": source,
        "importType": "archive.org"
    });

    // Add artist (music category standard field)
    if let Some(a) = artist {
        metadata["artist"] = serde_json::Value::String(a.to_string());
    }

    // Add track metadata as JSON string (Flagship expects stringified JSON array)
    if let Some(tracks) = track_metadata {
        metadata["trackMetadata"] = serde_json::Value::String(tracks.to_string());
    }

    // Add Archive.org identifier
    if let Some(id) = source.strip_prefix("archive.org:") {
        metadata["archiveOrgId"] = serde_json::Value::String(id.to_string());
    }

    // Parse and add license if provided
    if let Some(license_str) = license_json {
        if let Ok(license) = serde_json::from_str::<serde_json::Value>(license_str) {
            metadata["license"] = license;
        }
    }

    // Add date as releaseYear if provided (store full precision)
    if let Some(d) = date {
        metadata["releaseYear"] = serde_json::Value::String(d.to_string());
    }

    // Build the request body
    let request_body = CreateReleaseRequest {
        name: title.to_string(),
        category_id: "music".to_string(), // Default for Archive.org imports
        creator: artist.map(String::from), // Artist shown in endless view
        content_cid: content_cid.to_string(),
        thumbnail_cid: thumbnail_cid.map(String::from),
        status: "pending".to_string(), // Goes to moderation queue
        metadata: Some(metadata),
    };

    // Serialize request body
    let body = serde_json::to_string(&request_body)
        .map_err(|e| format!("Failed to serialize request: {}", e))?;

    // Sign "{timestamp}:{body}" as expected by Citadel Lens
    let timestamp = crate::auth::Auth::timestamp_millis();
    let message = format!("{}:{}", timestamp, body);
    let signature = auth.sign(message.as_bytes());

    // POST to Citadel Lens API
    let url = format!("{}/api/v1/releases", lens_url.trim_end_matches('/'));

    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("X-Public-Key", auth.public_key())
        .header("X-Timestamp", timestamp.to_string())
        .header("X-Signature", &signature)
        .body(body)
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

    let status = response.status();

    if !status.is_success() {
        let error_body = response.text().await.unwrap_or_default();

        // Try to parse as error response
        let error_msg = if let Ok(err) = serde_json::from_str::<CreateReleaseError>(&error_body) {
            err.error.unwrap_or_else(|| format!("HTTP {}", status.as_u16()))
        } else if status.as_u16() == 403 {
            "Not authorized to create releases. Ensure Librarian's public key is authorized as admin in Citadel Lens.".to_string()
        } else {
            format!("HTTP {}: {}", status.as_u16(), error_body)
        };

        error!(
            status = %status,
            error = %error_msg,
            "Failed to create release in Citadel Lens"
        );
        return Err(error_msg);
    }

    // Parse response - the release object is returned directly
    let release: CreateReleaseSuccess = response.json().await
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    info!(
        release_id = %release.id,
        title = %title,
        "Release created in Citadel Lens moderation queue"
    );

    Ok(release.id)
}

/// Try to create a release in Citadel Lens, logging but not failing on error.
///
/// This is a best-effort operation - the import is still successful even if
/// release creation fails. The user can manually create the release later.
pub async fn try_create_release(
    client: &Client,
    lens_url: Option<&str>,
    title: Option<&str>,
    artist: Option<&str>,
    content_cid: &str,
    thumbnail_cid: Option<&str>,
    source: &str,
    license_json: Option<&str>,
    date: Option<&str>,
    track_metadata: Option<&str>,
    auth: Option<&Auth>,
) -> Option<String> {
    // Check if Lens URL is configured
    let url = match lens_url {
        Some(u) if !u.is_empty() => u,
        _ => {
            warn!("No Citadel Lens URL configured - release not auto-created. Set LENS_URL env var.");
            return None;
        }
    };

    // Check if we have a title
    let title = match title {
        Some(t) if !t.is_empty() => t,
        _ => {
            warn!("No title extracted - using source identifier as title.");
            source
        }
    };

    // Check if auth is configured
    let auth = match auth {
        Some(a) => a,
        None => {
            warn!("No auth configured - cannot create release. Run 'librarian init' first.");
            return None;
        }
    };

    match create_release_in_lens(
        client,
        url,
        title,
        artist,
        content_cid,
        thumbnail_cid,
        source,
        license_json,
        date,
        track_metadata,
        auth,
    ).await {
        Ok(id) => Some(id),
        Err(e) => {
            warn!(error = %e, "Failed to create release in Citadel Lens - you can create it manually later");
            None
        }
    }
}
