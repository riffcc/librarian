//! Import executor for Archive.org items.
//!
//! Downloads files from Archive.org and uploads them to Archivist,
//! creating a directory manifest for the complete release.
//!
//! Extracts rich metadata from Archive.org including:
//! - Album/release title and artist
//! - Per-track titles for nice file naming ("01 - Track Name.mp3")
//! - License information for automatic categorization

use futures_util::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::auth::AuthCredentials;
use crate::crdt::JobResult;
use super::buffer::BufferPool;

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
    description: Option<serde_json::Value>,
    date: Option<String>,
    /// License URL from Archive.org (e.g., "http://creativecommons.org/licenses/by-nc-sa/4.0/")
    licenseurl: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ArchiveFileEntry {
    name: String,
    format: Option<String>,
    size: Option<String>,
    source: Option<String>,
    // Rich metadata from ID3/Vorbis tags (extracted by Archive.org)
    title: Option<String>,
    artist: Option<String>,
    album: Option<String>,
    track: Option<String>,
    length: Option<String>,
    genre: Option<String>,
}

/// Archivist directory response.
#[derive(Debug, Deserialize)]
struct DirectoryResponse {
    cid: String,
    #[serde(rename = "totalSize")]
    _total_size: u64,
    #[serde(rename = "filesCount")]
    _files_count: usize,
}

/// Extracted metadata from an Archive.org import.
#[derive(Debug, Clone, Serialize)]
pub struct ImportedMetadata {
    /// Source identifier (e.g., "archive.org:tou2016")
    pub source: String,
    /// Album/release title
    pub title: Option<String>,
    /// Artist/creator name
    pub artist: Option<String>,
    /// Release date
    pub date: Option<String>,
    /// License URL (e.g., Creative Commons)
    pub license_url: Option<String>,
    /// Detected license type
    pub license_type: Option<String>,
    /// Track listing
    pub tracks: Vec<TrackMetadata>,
}

/// Metadata for a single track.
#[derive(Debug, Clone, Serialize)]
pub struct TrackMetadata {
    /// Track number
    pub track_number: Option<u32>,
    /// Track title
    pub title: Option<String>,
    /// Track artist (if different from album)
    pub artist: Option<String>,
    /// Duration in seconds
    pub duration: Option<f64>,
    /// Final filename used in manifest
    pub filename: String,
    /// Original filename from source
    pub original_filename: String,
}

/// Extract first string from a JSON value (handles string or array).
fn extract_string(value: &Option<serde_json::Value>) -> Option<String> {
    match value {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Array(arr)) => arr
            .first()
            .and_then(|v| v.as_str())
            .map(String::from),
        _ => None,
    }
}

/// Parse track number from Archive.org format (e.g., "1", "01", "1/12").
fn parse_track_number(track: &Option<String>) -> Option<u32> {
    track.as_ref().and_then(|t| {
        // Handle "1/12" format
        let num_str = t.split('/').next().unwrap_or(t);
        num_str.trim().parse().ok()
    })
}

/// Parse duration from Archive.org length format (e.g., "3:45", "225.5").
fn parse_duration(length: &Option<String>) -> Option<f64> {
    length.as_ref().and_then(|l| {
        if l.contains(':') {
            // Format: "3:45" or "1:23:45"
            let parts: Vec<&str> = l.split(':').collect();
            match parts.len() {
                2 => {
                    let mins: f64 = parts[0].parse().ok()?;
                    let secs: f64 = parts[1].parse().ok()?;
                    Some(mins * 60.0 + secs)
                }
                3 => {
                    let hours: f64 = parts[0].parse().ok()?;
                    let mins: f64 = parts[1].parse().ok()?;
                    let secs: f64 = parts[2].parse().ok()?;
                    Some(hours * 3600.0 + mins * 60.0 + secs)
                }
                _ => None,
            }
        } else {
            l.parse().ok()
        }
    })
}

/// Generate a nice filename from track metadata.
/// Format: "01 - Track Title.ext" or original if no metadata.
fn generate_track_filename(file: &ArchiveFileEntry, track_num: Option<u32>) -> String {
    let extension = file.name
        .rsplit('.')
        .next()
        .map(|e| format!(".{}", e.to_lowercase()))
        .unwrap_or_default();

    // Try to use track title first
    if let Some(title) = &file.title {
        let track_prefix = track_num
            .map(|n| format!("{:02} - ", n))
            .unwrap_or_default();

        // Sanitize title for filesystem
        let clean_title: String = title
            .chars()
            .map(|c| match c {
                '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
                _ => c,
            })
            .collect();

        format!("{}{}{}", track_prefix, clean_title, extension)
    } else {
        // Fall back to original filename
        file.name.clone()
    }
}

/// Structured license info matching Flagship's format.
/// Stored as JSON string in release metadata.license field.
#[derive(Debug, Clone, Serialize)]
pub struct LicenseInfo {
    /// License type: cc0, cc-by, cc-by-sa, cc-by-nd, cc-by-nc, cc-by-nc-sa, cc-by-nc-nd, custom
    #[serde(rename = "type")]
    pub license_type: String,
    /// Version: 4.0, 3.0, 2.5, 2.0, 1.0, unknown
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Jurisdiction country code: us, uk, de, au, etc. Empty = International
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jurisdiction: Option<String>,
    /// Original license URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

/// Parse license info from URL, extracting type, version, and jurisdiction.
/// Returns structured LicenseInfo matching Flagship's format.
fn parse_license_info(license_url: &Option<String>) -> Option<LicenseInfo> {
    let url = license_url.as_ref()?;
    let url_lower = url.to_lowercase();

    if !url_lower.contains("creativecommons.org") {
        return None;
    }

    // Extract version from URL (e.g., "/4.0/", "/3.0/", "/2.5/")
    let version = extract_license_version(&url_lower);

    // Extract jurisdiction from URL (e.g., "/deed.de", "/de/", country-specific ports)
    let jurisdiction = extract_license_jurisdiction(&url_lower);

    // Detect license type - order matters (most specific first)
    let license_type = if url_lower.contains("/publicdomain/") || url_lower.contains("/zero/") || url_lower.contains("cc0") {
        "cc0"
    } else if url_lower.contains("by-nc-nd") {
        "cc-by-nc-nd"
    } else if url_lower.contains("by-nc-sa") {
        "cc-by-nc-sa"
    } else if url_lower.contains("by-nc") {
        "cc-by-nc"
    } else if url_lower.contains("by-nd") {
        "cc-by-nd"
    } else if url_lower.contains("by-sa") {
        "cc-by-sa"
    } else if url_lower.contains("/by/") || url_lower.contains("/by-") {
        "cc-by"
    } else {
        return None; // Unknown CC license type
    };

    Some(LicenseInfo {
        license_type: license_type.to_string(),
        version,
        jurisdiction,
        url: Some(url.clone()),
    })
}

/// Extract version number from CC license URL.
fn extract_license_version(url: &str) -> Option<String> {
    // Common version patterns: /4.0/, /3.0/, /2.5/, /2.0/, /1.0/
    let versions = ["4.0", "3.0", "2.5", "2.0", "1.0"];

    for v in versions {
        if url.contains(&format!("/{}/", v)) || url.ends_with(&format!("/{}", v)) {
            return Some(v.to_string());
        }
    }

    None
}

/// Extract jurisdiction country code from CC license URL.
/// Returns country code like "us", "uk", "de" or None for international.
fn extract_license_jurisdiction(url: &str) -> Option<String> {
    // Jurisdiction patterns in CC URLs:
    // - /licenses/by/3.0/us/
    // - /licenses/by/3.0/deed.de
    // - https://creativecommons.org/licenses/by-sa/2.0/uk/

    // Known jurisdiction codes (subset - most common)
    let jurisdictions = [
        "au", "at", "be", "br", "ca", "cl", "cn", "co", "hr", "cz",
        "dk", "ec", "fi", "fr", "de", "gr", "hk", "hu", "in", "ie",
        "il", "it", "jp", "kr", "my", "mx", "nl", "nz", "no", "pe",
        "ph", "pl", "pt", "ro", "rs", "sg", "za", "es", "se", "ch",
        "tw", "th", "uk", "us", "vn",
    ];

    // Check for /jurisdiction/ or /deed.jurisdiction patterns
    for j in jurisdictions {
        if url.contains(&format!("/{}/", j)) || url.ends_with(&format!("/{}", j)) {
            return Some(j.to_string());
        }
        if url.contains(&format!("/deed.{}", j)) {
            return Some(j.to_string());
        }
    }

    None
}

/// Audio formats to import.
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

/// Metadata file formats to include for attribution/provenance.
const METADATA_FORMATS: &[&str] = &[
    "Metadata",
    "Text",
];

/// Check if a file is an audio file.
fn is_audio_file(file: &ArchiveFileEntry) -> bool {
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

/// Check if a file is a metadata file to include for attribution.
fn is_metadata_file(file: &ArchiveFileEntry) -> bool {
    let name = file.name.to_lowercase();

    // Include _meta.xml, _files.xml for provenance
    if name.ends_with("_meta.xml") || name.ends_with("_files.xml") {
        return true;
    }

    // Include common metadata formats
    if let Some(format) = &file.format {
        METADATA_FORMATS.iter().any(|f| format.contains(f))
            && (name.ends_with(".xml") || name.ends_with(".json"))
    } else {
        false
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

/// Download and upload a single file to Archivist.
/// Returns (cid, size) on success.
async fn upload_file(
    client: &Client,
    archivist_url: &str,
    identifier: &str,
    file_name: &str,
    dest_name: &str,
    auth: Option<&AuthCredentials>,
    buffer_pool: &std::sync::Arc<BufferPool>,
) -> Option<(String, u64)> {
    info!(file = %file_name, dest = %dest_name, "Downloading from Archive.org");

    let download_url = format!(
        "https://archive.org/download/{}/{}",
        identifier,
        urlencoding::encode(file_name)
    );

    let response = match client.get(&download_url).send().await {
        Ok(r) => r,
        Err(e) => {
            warn!(file = %file_name, error = %e, "Failed to download, skipping");
            return None;
        }
    };

    if !response.status().is_success() {
        warn!(file = %file_name, status = %response.status(), "Failed to download, skipping");
        return None;
    }

    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| mime_from_name(file_name));

    // Acquire buffer slot
    let mut slot = buffer_pool.acquire().await;

    // Stream download into buffer
    let download_start = std::time::Instant::now();
    let mut stream = response.bytes_stream();

    while let Some(chunk_result) = stream.next().await {
        let chunk = match chunk_result {
            Ok(c) => c,
            Err(e) => {
                warn!(file = %file_name, error = %e, "Download error, skipping");
                return None;
            }
        };
        if let Err(e) = slot.write_chunk(&chunk) {
            warn!(file = %file_name, error = %e, "Buffer write error, skipping");
            return None;
        }
    }

    let size = slot.size();
    let download_secs = download_start.elapsed().as_secs_f64();
    let speed_mbps = if download_secs > 0.0 { (size as f64 / 1024.0 / 1024.0) / download_secs } else { 0.0 };

    info!(
        file = %file_name,
        size_mb = size / 1024 / 1024,
        speed_mbps = format!("{:.1}", speed_mbps),
        storage = slot.storage_type(),
        "Download complete"
    );

    // Upload to Archivist
    let bytes = match slot.into_bytes() {
        Ok(b) => b,
        Err(e) => {
            warn!(file = %file_name, error = %e, "Failed to read buffer, skipping");
            return None;
        }
    };

    let upload_url = format!("{}/api/archivist/v1/data", archivist_url);

    let mut request = client
        .post(&upload_url)
        .header("Content-Disposition", format!("attachment; filename=\"{}\"", dest_name))
        .body(bytes);

    if let Some(ct) = &content_type {
        request = request.header("Content-Type", ct);
    }

    if let Some(creds) = auth {
        request = request
            .header("X-Pubkey", &creds.pubkey)
            .header("X-Timestamp", creds.timestamp.to_string())
            .header("X-Signature", &creds.signature);
    }

    let upload_response = match request.send().await {
        Ok(resp) => resp,
        Err(e) => {
            warn!(file = %file_name, error = %e, "Failed to upload, skipping");
            return None;
        }
    };

    if !upload_response.status().is_success() {
        let status = upload_response.status();
        warn!(file = %file_name, status = %status, "Upload failed, skipping");
        return None;
    }

    let cid = match upload_response.text().await {
        Ok(text) => text.trim().to_string(),
        Err(e) => {
            warn!(file = %file_name, error = %e, "Failed to read CID, skipping");
            return None;
        }
    };

    info!(file = %file_name, cid = %cid, "Upload complete");
    Some((cid, size))
}

/// Execute an import from Archive.org to Archivist.
///
/// # Arguments
/// * `client` - HTTP client
/// * `archivist_url` - Base URL of Archivist API
/// * `identifier` - Archive.org item identifier
/// * `auth` - Optional authentication credentials for Archivist uploads
/// * `buffer_pool` - Shared buffer pool for downloads
/// * `on_progress` - Callback for progress updates (0.0-1.0, optional message)
pub async fn execute_import<F>(
    client: &Client,
    archivist_url: &str,
    identifier: &str,
    auth: Option<&AuthCredentials>,
    buffer_pool: &std::sync::Arc<BufferPool>,
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

    // 2. Extract album-level metadata
    let album_title = extract_string(&metadata.metadata.title);
    let album_artist = extract_string(&metadata.metadata.creator);
    let album_date = metadata.metadata.date.clone();
    let license_info = parse_license_info(&metadata.metadata.licenseurl);

    info!(
        identifier = %identifier,
        title = ?album_title,
        artist = ?album_artist,
        license = ?license_info,
        "Extracted metadata"
    );

    // 3. Filter to audio files and metadata files
    let audio_files: Vec<_> = metadata.files.iter().filter(|f| is_audio_file(f)).collect();
    let metadata_files: Vec<_> = metadata.files.iter().filter(|f| is_metadata_file(f)).collect();

    if audio_files.is_empty() {
        return Ok(JobResult::Error("No audio files found in item".to_string()));
    }

    info!(
        identifier = %identifier,
        audio_count = audio_files.len(),
        metadata_count = metadata_files.len(),
        "Found files to import"
    );

    on_progress(0.05, Some(format!("Found {} audio files", audio_files.len())));

    // 4. Download and upload files
    let mut uploaded_entries: Vec<DirectoryEntry> = Vec::new();
    let mut track_metadata: Vec<TrackMetadata> = Vec::new();
    let total_files = audio_files.len() + metadata_files.len();
    let mut file_index = 0;

    // Upload audio files with nice names
    for file in &audio_files {
        let file_progress = 0.05 + (0.85 * (file_index as f32 / total_files as f32));
        file_index += 1;

        let track_num = parse_track_number(&file.track);
        let nice_name = generate_track_filename(file, track_num);

        on_progress(file_progress, Some(format!("Uploading {}", nice_name)));

        if let Some((cid, size)) = upload_file(
            client,
            archivist_url,
            identifier,
            &file.name,
            &nice_name,
            auth,
            buffer_pool,
        ).await {
            uploaded_entries.push(DirectoryEntry {
                path: nice_name.clone(),
                cid,
                size,
                mimetype: mime_from_name(&file.name),
            });

            track_metadata.push(TrackMetadata {
                track_number: track_num,
                title: file.title.clone(),
                artist: file.artist.clone(),
                duration: parse_duration(&file.length),
                filename: nice_name,
                original_filename: file.name.clone(),
            });
        }
    }

    // Upload metadata files (for attribution/provenance)
    for file in &metadata_files {
        let file_progress = 0.05 + (0.85 * (file_index as f32 / total_files as f32));
        file_index += 1;

        // Keep original name for metadata files (prefixed with underscore)
        let dest_name = if file.name.starts_with('_') {
            file.name.clone()
        } else {
            format!("_{}", file.name)
        };

        on_progress(file_progress, Some(format!("Including {}", dest_name)));

        if let Some((cid, size)) = upload_file(
            client,
            archivist_url,
            identifier,
            &file.name,
            &dest_name,
            auth,
            buffer_pool,
        ).await {
            let mimetype = if file.name.ends_with(".xml") {
                Some("application/xml".to_string())
            } else if file.name.ends_with(".json") {
                Some("application/json".to_string())
            } else {
                Some("text/plain".to_string())
            };

            uploaded_entries.push(DirectoryEntry {
                path: dest_name,
                cid,
                size,
                mimetype,
            });
        }
    }

    if uploaded_entries.is_empty() {
        return Ok(JobResult::Error("Failed to upload any files".to_string()));
    }

    // Sort entries: audio files first (sorted by track), then metadata
    uploaded_entries.sort_by(|a, b| {
        let a_is_meta = a.path.starts_with('_');
        let b_is_meta = b.path.starts_with('_');
        match (a_is_meta, b_is_meta) {
            (true, false) => std::cmp::Ordering::Greater,
            (false, true) => std::cmp::Ordering::Less,
            _ => a.path.cmp(&b.path),
        }
    });

    on_progress(0.92, Some("Creating directory manifest".to_string()));

    // 5. Create directory manifest in Archivist
    let directory_url = format!("{}/api/archivist/v1/directory", archivist_url);

    let directory_request = serde_json::json!({
        "entries": uploaded_entries,
    });

    let mut request = client.post(&directory_url).json(&directory_request);

    if let Some(creds) = auth {
        request = request
            .header("X-Pubkey", &creds.pubkey)
            .header("X-Timestamp", creds.timestamp.to_string())
            .header("X-Signature", &creds.signature);
    }

    let directory_response = match request.send().await {
        Ok(resp) => resp,
        Err(e) => {
            let error_msg = if e.is_connect() {
                format!("Connection failed - Archivist may be down: {}", e)
            } else if e.is_timeout() {
                format!("Request timed out: {}", e)
            } else {
                format!("Request failed: {}", e)
            };
            return Ok(JobResult::Error(format!("Failed to create directory: {}", error_msg)));
        }
    };

    if !directory_response.status().is_success() {
        let status = directory_response.status();
        let error_body = directory_response.text().await.unwrap_or_default();
        let error_msg = if error_body.is_empty() {
            format!("HTTP {}: {}", status.as_u16(), status.canonical_reason().unwrap_or("Unknown error"))
        } else {
            format!("HTTP {}: {}", status.as_u16(), error_body)
        };
        return Ok(JobResult::Error(format!("Failed to create directory: {}", error_msg)));
    }

    let directory: DirectoryResponse = directory_response.json().await?;
    let audio_count = track_metadata.len();

    // Use ID3 tags first, fall back to Archive.org metadata
    // Precedence: ID3 >> Archive.org (default, configurable at Review stage)
    //
    // For album title: ID3 album tag > Archive.org title
    // For artist: ID3 artist tag > Archive.org creator
    let final_title = audio_files.iter()
        .find_map(|f| f.album.clone())  // First ID3 album tag
        .or(album_title.clone());  // Then Archive.org title

    let final_artist = audio_files.iter()
        .find_map(|f| f.artist.clone())  // First ID3 artist tag
        .or(album_artist.clone());  // Then Archive.org creator

    // Serialize license info to JSON for Flagship compatibility
    let license_json = license_info
        .as_ref()
        .and_then(|li| serde_json::to_string(li).ok());

    info!(
        identifier = %identifier,
        cid = %directory.cid,
        audio_files = audio_count,
        title = ?final_title,
        artist = ?final_artist,
        license = ?license_info,
        "Import complete"
    );

    on_progress(1.0, Some("Import complete".to_string()));

    Ok(JobResult::Import {
        files_imported: audio_count,
        directory_cid: directory.cid,
        source: format!("archive.org:{}", identifier),
        title: final_title,
        artist: final_artist,
        date: album_date,
        license: license_json,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_file(name: &str, format: Option<&str>) -> ArchiveFileEntry {
        ArchiveFileEntry {
            name: name.to_string(),
            format: format.map(String::from),
            size: None,
            source: None,
            title: None,
            artist: None,
            album: None,
            track: None,
            length: None,
            genre: None,
        }
    }

    #[test]
    fn test_is_audio_file() {
        assert!(is_audio_file(&make_file("track.mp3", Some("VBR MP3"))));
        assert!(is_audio_file(&make_file("track.flac", Some("FLAC"))));
        assert!(is_audio_file(&make_file("track.ogg", None)));  // By extension
        assert!(!is_audio_file(&make_file("meta.xml", Some("Metadata"))));
    }

    #[test]
    fn test_is_metadata_file() {
        assert!(is_metadata_file(&make_file("item_meta.xml", Some("Metadata"))));
        assert!(is_metadata_file(&make_file("item_files.xml", Some("Text"))));
        assert!(!is_metadata_file(&make_file("track.mp3", Some("VBR MP3"))));
    }

    #[test]
    fn test_mime_from_name() {
        assert_eq!(mime_from_name("track.mp3"), Some("audio/mpeg".to_string()));
        assert_eq!(mime_from_name("track.FLAC"), Some("audio/flac".to_string()));
        assert_eq!(mime_from_name("track.ogg"), Some("audio/ogg".to_string()));
        assert_eq!(mime_from_name("track.txt"), None);
    }

    #[test]
    fn test_parse_track_number() {
        assert_eq!(parse_track_number(&Some("1".to_string())), Some(1));
        assert_eq!(parse_track_number(&Some("01".to_string())), Some(1));
        assert_eq!(parse_track_number(&Some("5/12".to_string())), Some(5));
        assert_eq!(parse_track_number(&None), None);
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration(&Some("3:45".to_string())), Some(225.0));
        assert_eq!(parse_duration(&Some("1:23:45".to_string())), Some(5025.0));
        assert_eq!(parse_duration(&Some("180.5".to_string())), Some(180.5));
        assert_eq!(parse_duration(&None), None);
    }

    #[test]
    fn test_generate_track_filename() {
        let mut file = make_file("original_file.mp3", Some("MP3"));
        file.title = Some("My Great Song".to_string());
        file.track = Some("3".to_string());

        let name = generate_track_filename(&file, Some(3));
        assert_eq!(name, "03 - My Great Song.mp3");

        // Without track number
        let name = generate_track_filename(&file, None);
        assert_eq!(name, "My Great Song.mp3");

        // Without title - use original
        file.title = None;
        let name = generate_track_filename(&file, Some(1));
        assert_eq!(name, "original_file.mp3");
    }

    #[test]
    fn test_parse_license_info() {
        // CC BY-SA 4.0 International
        let info = parse_license_info(&Some("http://creativecommons.org/licenses/by-sa/4.0/".to_string()));
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.license_type, "cc-by-sa");
        assert_eq!(info.version, Some("4.0".to_string()));
        assert_eq!(info.jurisdiction, None);

        // CC0 1.0
        let info = parse_license_info(&Some("http://creativecommons.org/publicdomain/zero/1.0/".to_string()));
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.license_type, "cc0");
        assert_eq!(info.version, Some("1.0".to_string()));

        // CC BY 3.0 US (with jurisdiction)
        let info = parse_license_info(&Some("http://creativecommons.org/licenses/by/3.0/us/".to_string()));
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.license_type, "cc-by");
        assert_eq!(info.version, Some("3.0".to_string()));
        assert_eq!(info.jurisdiction, Some("us".to_string()));

        // CC BY-NC-SA 2.5 (older version)
        let info = parse_license_info(&Some("http://creativecommons.org/licenses/by-nc-sa/2.5/".to_string()));
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.license_type, "cc-by-nc-sa");
        assert_eq!(info.version, Some("2.5".to_string()));

        // Non-CC URL returns None
        assert!(parse_license_info(&Some("http://example.com/license".to_string())).is_none());
        assert!(parse_license_info(&None).is_none());
    }

    #[test]
    fn test_extract_license_version() {
        assert_eq!(extract_license_version("http://creativecommons.org/licenses/by/4.0/"), Some("4.0".to_string()));
        assert_eq!(extract_license_version("http://creativecommons.org/licenses/by/3.0/us/"), Some("3.0".to_string()));
        assert_eq!(extract_license_version("http://creativecommons.org/licenses/by/2.5/"), Some("2.5".to_string()));
        assert_eq!(extract_license_version("http://example.com/"), None);
    }

    #[test]
    fn test_extract_license_jurisdiction() {
        assert_eq!(extract_license_jurisdiction("http://creativecommons.org/licenses/by/3.0/us/"), Some("us".to_string()));
        assert_eq!(extract_license_jurisdiction("http://creativecommons.org/licenses/by/3.0/de/"), Some("de".to_string()));
        assert_eq!(extract_license_jurisdiction("http://creativecommons.org/licenses/by/4.0/deed.uk"), Some("uk".to_string()));
        assert_eq!(extract_license_jurisdiction("http://creativecommons.org/licenses/by/4.0/"), None); // International
    }
}
