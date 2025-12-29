//! Import executor for Archive.org items.
//!
//! Downloads files from Archive.org and uploads them to Archivist,
//! creating a directory manifest for the complete release.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::crdt::JobResult;

/// File entry for directory manifest.
#[derive(Debug, Serialize)]
struct DirectoryEntry {
    path: String,
    cid: String,
    size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    mimetype: Option<String>,
}

/// Archive.org metadata response.
#[derive(Debug, Deserialize)]
struct ArchiveMetadata {
    metadata: ArchiveItemMetadata,
    files: Vec<ArchiveFileEntry>,
}

#[derive(Debug, Deserialize)]
struct ArchiveItemMetadata {
    identifier: String,
    title: Option<serde_json::Value>,
    creator: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ArchiveFileEntry {
    name: String,
    format: Option<String>,
    size: Option<String>,
    source: Option<String>,
}

/// Archivist upload response.
#[derive(Debug, Deserialize)]
struct UploadResponse {
    // Archivist returns the CID as plain text, but we parse it
}

/// Archivist directory response.
#[derive(Debug, Deserialize)]
struct DirectoryResponse {
    cid: String,
    #[serde(rename = "totalSize")]
    total_size: u64,
    #[serde(rename = "filesCount")]
    files_count: usize,
}

/// Audio formats to import (skip metadata/text files).
const AUDIO_FORMATS: &[&str] = &[
    "VBR MP3",
    "MP3",
    "Ogg Vorbis",
    "FLAC",
    "Apple Lossless Audio",
    "AIFF",
    "WAV",
    "Opus",
    "AAC",
    "M4A",
];

/// Check if a file should be imported based on format.
fn should_import(file: &ArchiveFileEntry) -> bool {
    if let Some(format) = &file.format {
        AUDIO_FORMATS.iter().any(|f| format.contains(f))
    } else {
        // Check extension if no format specified
        let name = file.name.to_lowercase();
        name.ends_with(".mp3")
            || name.ends_with(".flac")
            || name.ends_with(".ogg")
            || name.ends_with(".opus")
            || name.ends_with(".m4a")
            || name.ends_with(".aac")
            || name.ends_with(".wav")
            || name.ends_with(".aiff")
    }
}

/// Get MIME type from filename.
fn mime_from_name(name: &str) -> Option<String> {
    let name = name.to_lowercase();
    if name.ends_with(".mp3") {
        Some("audio/mpeg".to_string())
    } else if name.ends_with(".flac") {
        Some("audio/flac".to_string())
    } else if name.ends_with(".ogg") {
        Some("audio/ogg".to_string())
    } else if name.ends_with(".opus") {
        Some("audio/opus".to_string())
    } else if name.ends_with(".m4a") || name.ends_with(".aac") {
        Some("audio/mp4".to_string())
    } else if name.ends_with(".wav") {
        Some("audio/wav".to_string())
    } else if name.ends_with(".aiff") {
        Some("audio/aiff".to_string())
    } else {
        None
    }
}

/// Execute an import from Archive.org to Archivist.
///
/// # Arguments
/// * `client` - HTTP client
/// * `archivist_url` - Base URL of Archivist API
/// * `identifier` - Archive.org item identifier
/// * `auth_pubkey` - Optional pre-authorized public key for Archivist uploads
/// * `on_progress` - Callback for progress updates (0.0-1.0, optional message)
pub async fn execute_import<F>(
    client: &Client,
    archivist_url: &str,
    identifier: &str,
    auth_pubkey: Option<&str>,
    mut on_progress: F,
) -> anyhow::Result<JobResult>
where
    F: FnMut(f32, Option<String>),
{
    info!(identifier = %identifier, "Starting import from Archive.org");
    on_progress(0.0, Some("Fetching metadata".to_string()));

    // 1. Fetch item metadata from Archive.org
    let metadata_url = format!("https://archive.org/metadata/{}", identifier);
    let metadata: ArchiveMetadata = client
        .get(&metadata_url)
        .send()
        .await?
        .json()
        .await?;

    // 2. Filter to audio files only
    let audio_files: Vec<_> = metadata
        .files
        .iter()
        .filter(|f| should_import(f))
        .collect();

    if audio_files.is_empty() {
        return Ok(JobResult::Error("No audio files found in item".to_string()));
    }

    info!(
        identifier = %identifier,
        file_count = audio_files.len(),
        "Found audio files to import"
    );

    on_progress(0.05, Some(format!("Found {} audio files", audio_files.len())));

    // 3. Download each file and upload to Archivist
    let mut uploaded_entries: Vec<DirectoryEntry> = Vec::new();
    let total_files = audio_files.len();

    for (i, file) in audio_files.iter().enumerate() {
        let file_progress = 0.05 + (0.85 * (i as f32 / total_files as f32));
        on_progress(file_progress, Some(format!("Uploading {}", file.name)));

        debug!(file = %file.name, "Downloading from Archive.org");

        // Download file from Archive.org
        let download_url = format!(
            "https://archive.org/download/{}/{}",
            identifier,
            urlencoding::encode(&file.name)
        );

        let response = client.get(&download_url).send().await?;

        if !response.status().is_success() {
            warn!(
                file = %file.name,
                status = %response.status(),
                "Failed to download file, skipping"
            );
            continue;
        }

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .or_else(|| mime_from_name(&file.name));

        let bytes = response.bytes().await?;
        let size = bytes.len() as u64;

        debug!(file = %file.name, size = size, "Downloaded, uploading to Archivist");

        // Upload to Archivist
        let upload_url = format!("{}/api/archivist/v1/data", archivist_url);

        let mut request = client
            .post(&upload_url)
            .header(
                "Content-Disposition",
                format!("attachment; filename=\"{}\"", file.name),
            )
            .body(bytes);

        if let Some(ct) = &content_type {
            request = request.header("Content-Type", ct);
        }

        // Add auth header for upload permission
        if let Some(pubkey) = auth_pubkey {
            request = request.header("X-Pubkey", pubkey);
        }

        let upload_response = request.send().await?;

        if !upload_response.status().is_success() {
            let error = upload_response.text().await?;
            warn!(
                file = %file.name,
                error = %error,
                "Failed to upload file, skipping"
            );
            continue;
        }

        // Archivist returns CID as plain text
        let cid = upload_response.text().await?.trim().to_string();

        debug!(file = %file.name, cid = %cid, "Uploaded to Archivist");

        uploaded_entries.push(DirectoryEntry {
            path: file.name.clone(),
            cid,
            size,
            mimetype: content_type,
        });
    }

    if uploaded_entries.is_empty() {
        return Ok(JobResult::Error("Failed to upload any files".to_string()));
    }

    on_progress(0.90, Some("Creating directory manifest".to_string()));

    // 4. Create directory manifest in Archivist
    let directory_url = format!("{}/api/archivist/v1/directory", archivist_url);

    let directory_request = serde_json::json!({
        "entries": uploaded_entries,
    });

    let mut request = client
        .post(&directory_url)
        .json(&directory_request);

    // Add auth header for directory creation
    if let Some(pubkey) = auth_pubkey {
        request = request.header("X-Pubkey", pubkey);
    }

    let directory_response = request.send().await?;

    if !directory_response.status().is_success() {
        let error = directory_response.text().await?;
        return Ok(JobResult::Error(format!(
            "Failed to create directory: {}",
            error
        )));
    }

    let directory: DirectoryResponse = directory_response.json().await?;

    info!(
        identifier = %identifier,
        cid = %directory.cid,
        files = directory.files_count,
        size = directory.total_size,
        "Import complete"
    );

    on_progress(1.0, Some("Import complete".to_string()));

    Ok(JobResult::Import {
        files_imported: uploaded_entries.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_import() {
        let mp3 = ArchiveFileEntry {
            name: "track.mp3".to_string(),
            format: Some("VBR MP3".to_string()),
            size: Some("1234567".to_string()),
            source: Some("original".to_string()),
        };
        assert!(should_import(&mp3));

        let flac = ArchiveFileEntry {
            name: "track.flac".to_string(),
            format: Some("FLAC".to_string()),
            size: Some("12345678".to_string()),
            source: Some("original".to_string()),
        };
        assert!(should_import(&flac));

        let xml = ArchiveFileEntry {
            name: "meta.xml".to_string(),
            format: Some("Metadata".to_string()),
            size: Some("1234".to_string()),
            source: Some("original".to_string()),
        };
        assert!(!should_import(&xml));
    }

    #[test]
    fn test_mime_from_name() {
        assert_eq!(mime_from_name("track.mp3"), Some("audio/mpeg".to_string()));
        assert_eq!(mime_from_name("track.FLAC"), Some("audio/flac".to_string()));
        assert_eq!(mime_from_name("track.ogg"), Some("audio/ogg".to_string()));
        assert_eq!(mime_from_name("track.txt"), None);
    }
}
