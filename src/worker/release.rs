//! Release creation for Citadel Lens.
//!
//! After an import completes, creates a release in the configured
//! Citadel Lens instance with the extracted metadata.
//!
//! Metadata format matches Flagship's expected schema:
//! - `metadata.author` - artist name
//! - `metadata.trackMetadata` - JSON array of track info
//! - `metadata.audioQuality` - quality badge info (format, bitrate, etc.)
//! - `thumbnailCID` - cover art CID (WebP)

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn, error, debug};

use crate::auth::Auth;
use crate::crdt::AudioQuality;

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
/// * `audio_quality` - Optional audio quality metadata for codec badge
/// * `quality_tiers` - Optional quality ladder mapping (tier name -> CID)
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
    audio_quality: Option<&AudioQuality>,
    quality_tiers: Option<&std::collections::HashMap<String, String>>,
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

    // Add audio quality metadata for codec badge display
    if let Some(quality) = audio_quality {
        if let Ok(quality_json) = serde_json::to_value(quality) {
            metadata["audioQuality"] = quality_json;
        }
    }

    // Add quality ladder (available tiers with their CIDs) for quality switching
    if let Some(tiers) = quality_tiers {
        if !tiers.is_empty() {
            if let Ok(tiers_json) = serde_json::to_value(tiers) {
                metadata["qualityLadder"] = tiers_json;
            }
        }
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

/// Verify an existing release and update only what's different.
///
/// Compares metadata fields and only sends updates for mismatches.
/// Returns Ok(true) if release was updated, Ok(false) if already correct.
async fn verify_existing_release(
    client: &Client,
    lens_url: &str,
    release_id: &str,
    new_content_cid: &str,  // NEW: the correct contentCID from this import
    new_title: &str,
    new_artist: Option<&str>,
    new_thumbnail_cid: Option<&str>,
    new_license_json: Option<&str>,
    new_date: Option<&str>,
    new_track_metadata: Option<&str>,
    new_audio_quality: Option<&AudioQuality>,
    new_quality_tiers: Option<&std::collections::HashMap<String, String>>,
    auth: &Auth,
) -> Result<bool, String> {
    // First, fetch the existing release to compare
    let fetch_url = format!("{}/api/v1/releases/{}", lens_url.trim_end_matches('/'), release_id);

    let existing = match client.get(&fetch_url).send().await {
        Ok(resp) if resp.status().is_success() => {
            resp.json::<serde_json::Value>().await.ok()
        }
        Ok(resp) => {
            debug!(status = %resp.status(), "Failed to fetch existing release for comparison");
            None
        }
        Err(e) => {
            debug!(error = %e, "Failed to fetch existing release");
            None
        }
    };

    // Build what we want the metadata to look like
    let mut desired_metadata = serde_json::Map::new();

    if let Some(a) = new_artist {
        desired_metadata.insert("author".to_string(), serde_json::Value::String(a.to_string()));
    }
    if let Some(license) = new_license_json {
        desired_metadata.insert("license".to_string(), serde_json::Value::String(license.to_string()));
    }
    if let Some(date_str) = new_date {
        desired_metadata.insert("releaseYear".to_string(), serde_json::Value::String(date_str.to_string()));
    }
    if let Some(tracks) = new_track_metadata {
        desired_metadata.insert("trackMetadata".to_string(), serde_json::Value::String(tracks.to_string()));
    }
    if let Some(quality) = new_audio_quality {
        if let Ok(v) = serde_json::to_value(quality) {
            desired_metadata.insert("audioQuality".to_string(), v);
        }
    }
    if let Some(tiers) = new_quality_tiers {
        if let Ok(v) = serde_json::to_value(tiers) {
            desired_metadata.insert("qualityLadder".to_string(), v);
        }
    }

    // Compare with existing and build update payload with only differences
    let mut updates = serde_json::Map::new();
    let mut metadata_updates = serde_json::Map::new();

    if let Some(ref existing) = existing {
        let existing_meta = existing.get("metadata").and_then(|m| m.as_object());

        // Check contentCID - THIS IS THE KEY CHECK
        // If files were re-uploaded (e.g., with fixed deduplication), update the CID
        let existing_cid = existing.get("contentCID").and_then(|v| v.as_str()).unwrap_or("");
        if existing_cid != new_content_cid {
            info!(
                old_cid = %existing_cid,
                new_cid = %new_content_cid,
                "Content CID differs - files were re-uploaded, updating release"
            );
            updates.insert("contentCID".to_string(), serde_json::Value::String(new_content_cid.to_string()));
        }

        // Check title
        let existing_name = existing.get("name").and_then(|v| v.as_str()).unwrap_or("");
        if existing_name != new_title {
            debug!(old = %existing_name, new = %new_title, "Title differs");
            updates.insert("name".to_string(), serde_json::Value::String(new_title.to_string()));
        }

        // Check thumbnail
        let existing_thumb = existing.get("thumbnailCID").and_then(|v| v.as_str());
        if new_thumbnail_cid != existing_thumb {
            if let Some(thumb) = new_thumbnail_cid {
                debug!(old = ?existing_thumb, new = %thumb, "Thumbnail differs");
                updates.insert("thumbnailCID".to_string(), serde_json::Value::String(thumb.to_string()));
            }
        }

        // Check each metadata field
        for (key, new_value) in &desired_metadata {
            let existing_value = existing_meta.and_then(|m| m.get(key));

            let differs = match existing_value {
                None => true, // Field missing
                Some(existing) => existing != new_value,
            };

            if differs {
                debug!(field = %key, "Metadata field differs or missing");
                metadata_updates.insert(key.clone(), new_value.clone());
            }
        }

        // Verify track count matches (integrity check)
        if let Some(tracks_json) = new_track_metadata {
            if let Ok(new_tracks) = serde_json::from_str::<Vec<serde_json::Value>>(tracks_json) {
                let existing_track_count = existing_meta
                    .and_then(|m| m.get("trackMetadata"))
                    .and_then(|v| v.as_str())
                    .and_then(|s| serde_json::from_str::<Vec<serde_json::Value>>(s).ok())
                    .map(|t| t.len())
                    .unwrap_or(0);

                if existing_track_count != new_tracks.len() {
                    info!(
                        existing = existing_track_count,
                        new = new_tracks.len(),
                        "Track count mismatch - updating"
                    );
                }
            }
        }
    } else {
        // Couldn't fetch existing - update everything to be safe
        warn!(release_id = %release_id, "Couldn't fetch existing release - updating all fields");
        updates.insert("contentCID".to_string(), serde_json::Value::String(new_content_cid.to_string()));
        updates.insert("name".to_string(), serde_json::Value::String(new_title.to_string()));
        if let Some(thumb) = new_thumbnail_cid {
            updates.insert("thumbnailCID".to_string(), serde_json::Value::String(thumb.to_string()));
        }
        metadata_updates = desired_metadata.into_iter().collect();
    }

    // If nothing to update, we're done
    if updates.is_empty() && metadata_updates.is_empty() {
        info!(release_id = %release_id, "Release verified - no changes needed");
        return Ok(false);
    }

    // Add metadata updates to payload
    if !metadata_updates.is_empty() {
        updates.insert("metadata".to_string(), serde_json::Value::Object(metadata_updates));
    }

    info!(
        release_id = %release_id,
        fields = ?updates.keys().collect::<Vec<_>>(),
        "Updating release with changed fields"
    );

    // Send update
    let url = format!("{}/api/v1/releases/{}", lens_url.trim_end_matches('/'), release_id);
    let body = serde_json::to_string(&serde_json::Value::Object(updates)).map_err(|e| e.to_string())?;
    let timestamp = Auth::timestamp_millis();
    let message = format!("{}:{}", timestamp, body);
    let signature = auth.sign(message.as_bytes());

    let response = client
        .put(&url)
        .header("Content-Type", "application/json")
        .header("X-Public-Key", auth.public_key())
        .header("X-Timestamp", timestamp.to_string())
        .header("X-Signature", &signature)
        .body(body)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if response.status().is_success() {
        info!(release_id = %release_id, "Successfully updated release");
        Ok(true)
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(format!("HTTP {}: {}", status, body))
    }
}

/// Check if a pending release already exists that matches our import.
///
/// Checks in priority order:
/// 1. Same contentCID (exact match - same files)
/// 2. Same source (e.g., "archive.org:tou247" - same origin, possibly different files)
/// 3. Same name + artist (fallback for non-Archive.org sources)
///
/// Returns the existing release ID if found, None otherwise.
async fn check_duplicate_pending_release(
    client: &Client,
    lens_url: &str,
    content_cid: &str,
    source: &str,
    title: &str,
    artist: Option<&str>,
    auth: &Auth,
) -> Option<String> {
    let url = format!("{}/api/v1/releases/pending", lens_url.trim_end_matches('/'));

    // Pending releases endpoint requires authentication
    let timestamp = Auth::timestamp_millis();
    let message = format!("{}:", timestamp); // GET request, no body
    let signature = auth.sign(message.as_bytes());

    let response = match client
        .get(&url)
        .header("X-Public-Key", auth.public_key())
        .header("X-Timestamp", timestamp.to_string())
        .header("X-Signature", &signature)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "Failed to fetch pending releases - duplicate check skipped!");
            return None;
        }
    };

    if !response.status().is_success() {
        warn!(status = %response.status(), url = %url, "Failed to fetch pending releases - duplicate check skipped!");
        return None;
    }

    // Parse response as array of releases
    let releases: Vec<serde_json::Value> = match response.json().await {
        Ok(r) => r,
        Err(e) => {
            debug!(error = %e, "Failed to parse pending releases response");
            return None;
        }
    };

    // Extract Archive.org identifier if this is an Archive.org source
    let archive_org_id = source.strip_prefix("archive.org:");

    // Check each release for matches
    for release in &releases {
        let release_id = match release.get("id").and_then(|v| v.as_str()) {
            Some(id) => id,
            None => continue,
        };

        // Priority 1: Same contentCID (exact file match)
        if let Some(cid) = release.get("contentCID").and_then(|v| v.as_str()) {
            if cid == content_cid {
                debug!(release_id = %release_id, "Duplicate found by contentCID");
                return Some(release_id.to_string());
            }
        }

        // Priority 2: Same source (Archive.org identifier)
        if let Some(metadata) = release.get("metadata").and_then(|m| m.as_object()) {
            // Check metadata.source
            if let Some(existing_source) = metadata.get("source").and_then(|v| v.as_str()) {
                if existing_source == source {
                    debug!(release_id = %release_id, source = %source, "Duplicate found by source");
                    return Some(release_id.to_string());
                }
            }

            // Check metadata.archiveOrgId
            if let Some(archive_id) = archive_org_id {
                if let Some(existing_archive_id) = metadata.get("archiveOrgId").and_then(|v| v.as_str()) {
                    if existing_archive_id == archive_id {
                        debug!(release_id = %release_id, archive_id = %archive_id, "Duplicate found by archiveOrgId");
                        return Some(release_id.to_string());
                    }
                }
            }
        }
    }

    // Priority 3: Same name + artist (fallback for non-Archive.org)
    // Only use this if we don't have an Archive.org source
    if archive_org_id.is_none() {
        for release in &releases {
            let release_id = match release.get("id").and_then(|v| v.as_str()) {
                Some(id) => id,
                None => continue,
            };

            let release_name = release.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let release_artist = release.get("metadata")
                .and_then(|m| m.get("author"))
                .and_then(|v| v.as_str());

            if release_name == title && release_artist == artist {
                debug!(release_id = %release_id, title = %title, "Duplicate found by name+artist");
                return Some(release_id.to_string());
            }
        }
    }

    None
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
    audio_quality: Option<&AudioQuality>,
    quality_tiers: Option<&std::collections::HashMap<String, String>>,
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

    // Check for duplicate: does a pending release already exist?
    // Checks by contentCID, source (archive.org ID), or name+artist
    if let Some(existing_id) = check_duplicate_pending_release(client, url, content_cid, source, title, artist, auth).await {
        info!(
            release_id = %existing_id,
            content_cid = %content_cid,
            source = %source,
            "Release already exists in moderation queue - verifying"
        );

        // Verify the existing release - update fields that differ, including contentCID
        match verify_existing_release(
            client,
            url,
            &existing_id,
            content_cid,  // NEW: pass the new contentCID to compare/update
            title,
            artist,
            thumbnail_cid,
            license_json,
            date,
            track_metadata,
            audio_quality,
            quality_tiers,
            auth,
        ).await {
            Ok(true) => info!(release_id = %existing_id, "Release updated with new metadata/files"),
            Ok(false) => info!(release_id = %existing_id, "Release verified - already up to date"),
            Err(e) => warn!(error = %e, release_id = %existing_id, "Failed to verify release"),
        }

        return Some(existing_id);
    }

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
        audio_quality,
        quality_tiers,
        auth,
    ).await {
        Ok(id) => Some(id),
        Err(e) => {
            warn!(error = %e, "Failed to create release in Citadel Lens - you can create it manually later");
            None
        }
    }
}
