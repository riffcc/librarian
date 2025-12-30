//! Source import executor for URLs and CIDs.
//!
//! Downloads content from URLs, IPFS gateways, or Archivist and re-uploads
//! to the target Archivist instance. Supports single files and directories.

use std::sync::Arc;

use futures_util::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::auth::AuthCredentials;
use crate::crdt::JobResult;
use super::buffer::BufferPool;
use super::metadata::{
    extract_audio_metadata, extract_audio_quality, extract_track_metadata, extract_embedded_cover,
    generate_golden_filename, generate_golden_dirname, is_audio_file as is_audio_filename,
    is_cover_image, TrackMetadata,
};
use super::cover::{process_cover_bytes, upload_processed_cover, CoverSource};

/// Default IPFS gateway for CID resolution.
pub const DEFAULT_IPFS_GATEWAY: &str = "https://cdn.riff.cc/ipfs/";

/// Metadata gathered during directory import for standardized naming.
#[derive(Debug, Default)]
struct ImportMetadata {
    /// Album artist (from audio tags or provided)
    artist: Option<String>,
    /// Album title (from audio tags or provided)
    album: Option<String>,
    /// Release year (from audio tags or provided)
    year: Option<u32>,
    /// Track metadata for each audio file (keyed by original filename)
    tracks: Vec<(String, TrackMetadata)>,
    /// Cover art CID if found and uploaded
    cover_cid: Option<String>,
    /// Fallback cover CID (JPG/PNG)
    cover_fallback_cid: Option<String>,
    /// Audio quality of the primary format
    audio_quality: Option<crate::crdt::AudioQuality>,
}

/// Default Archivist gateway for CID resolution.
pub const DEFAULT_ARCHIVIST_GATEWAY: &str = "https://archivist.riff.cc/api/archivist/v1/data/";

/// File entry for directory manifest.
#[derive(Debug, Serialize)]
struct DirectoryEntry {
    path: String,
    cid: String,
    size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    mimetype: Option<String>,
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

/// Source type detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceType {
    /// Direct HTTP/HTTPS URL.
    Url,
    /// IPFS CID (Qm..., bafy...).
    IpfsCid,
    /// Archivist CID (zD..., zE...).
    ArchivistCid,
}

impl SourceType {
    /// Detect source type from string.
    pub fn detect(source: &str) -> Option<Self> {
        let source = source.trim();

        if source.starts_with("http://") || source.starts_with("https://") {
            return Some(Self::Url);
        }

        // IPFS CIDv0 or CIDv1
        if source.starts_with("Qm") && source.len() == 46 {
            return Some(Self::IpfsCid);
        }
        if source.starts_with("bafy") {
            return Some(Self::IpfsCid);
        }

        // Archivist CIDs
        if source.starts_with("zD") || source.starts_with("zE") {
            return Some(Self::ArchivistCid);
        }

        None
    }
}

/// Progress callback with human-readable speed.
pub type ProgressCallback = Box<dyn FnMut(f32, Option<String>, Option<String>) + Send>;

/// Format bytes with appropriate unit.
fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * 1024 * 1024;

    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format speed with appropriate unit.
fn format_speed(bytes_per_sec: f64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

    if bytes_per_sec >= GIB {
        format!("{:.2} GiB/s", bytes_per_sec / GIB)
    } else if bytes_per_sec >= MIB {
        format!("{:.2} MiB/s", bytes_per_sec / MIB)
    } else if bytes_per_sec >= KIB {
        format!("{:.2} KiB/s", bytes_per_sec / KIB)
    } else {
        format!("{:.0} B/s", bytes_per_sec)
    }
}

/// Parse IPFS gateway HTML directory listing to extract file links.
///
/// The HTML structure for file entries is:
/// ```html
/// <a href="/ipfs/{base_cid}/{encoded_filename}">{filename}</a>
/// ```
///
/// We need to match only links that start with `/ipfs/{base_cid}/` to avoid
/// picking up external links, navigation, data URIs, or individual file CID links.
fn parse_ipfs_html_directory(html: &str, base_cid: &str) -> Vec<(String, u64)> {
    let mut files = Vec::new();

    // Build the prefix pattern for this directory's file links
    let dir_prefix = format!("/ipfs/{}/", base_cid);

    // Simple parsing for <a href="...">text</a>
    let mut pos = 0;
    while let Some(href_start) = html[pos..].find("href=\"") {
        let href_start = pos + href_start + 6;
        if let Some(href_end) = html[href_start..].find('"') {
            let href = &html[href_start..href_start + href_end];

            // Only process links that are within this directory
            if let Some(rest) = href.strip_prefix(&dir_prefix) {
                // Extract just the filename part (before any query params)
                let filename = rest.split('?').next().unwrap_or(rest);

                // URL decode
                let filename = urlencoding::decode(filename)
                    .map(|s| s.into_owned())
                    .unwrap_or_else(|_| filename.to_string());

                // Skip empty filenames and navigation
                if !filename.is_empty()
                    && filename != ".."
                    && filename != "."
                    && !filename.starts_with('#')
                {
                    // Deduplicate - only add if not already present
                    if !files.iter().any(|(name, _)| name == &filename) {
                        files.push((filename, 0u64));
                    }
                }
            }

            pos = href_start + href_end;
        } else {
            break;
        }
    }

    files
}

/// Check if response looks like HTML.
fn is_html_response(content_type: Option<&str>, body_preview: &[u8]) -> bool {
    if let Some(ct) = content_type {
        if ct.contains("text/html") {
            return true;
        }
    }
    if body_preview.len() >= 15 {
        let preview = String::from_utf8_lossy(&body_preview[..body_preview.len().min(500)]);
        return preview.contains("<!DOCTYPE")
            || preview.contains("<html")
            || preview.contains("<HTML")
            || preview.contains("Index of /ipfs");
    }
    false
}

/// Execute a source import job.
///
/// # Arguments
/// * `client` - HTTP client
/// * `archivist_url` - Base URL of target Archivist API
/// * `source` - Source URL or CID
/// * `gateway` - Optional gateway override for CID resolution
/// * `auth` - Optional authentication credentials for Archivist uploads
/// * `buffer_pool` - Shared buffer pool for downloads
/// * `on_progress` - Callback for progress updates (progress 0-1, message, speed)
pub async fn execute_source_import<F>(
    client: &Client,
    archivist_url: &str,
    source: &str,
    gateway: Option<&str>,
    auth: Option<&AuthCredentials>,
    buffer_pool: &Arc<BufferPool>,
    mut on_progress: F,
) -> anyhow::Result<JobResult>
where
    F: FnMut(f32, Option<String>, Option<String>),
{
    let source_type = SourceType::detect(source)
        .ok_or_else(|| anyhow::anyhow!("Invalid source format: {}", source))?;

    info!(source = %source, source_type = ?source_type, "Starting source import");
    on_progress(0.0, Some("Starting import".to_string()), None);

    // Build fetch URL based on source type
    let fetch_url = match source_type {
        SourceType::Url => source.to_string(),
        SourceType::IpfsCid => {
            let gw = gateway.unwrap_or(DEFAULT_IPFS_GATEWAY);
            format!("{}{}", gw, source)
        }
        SourceType::ArchivistCid => {
            let gw = gateway.unwrap_or(DEFAULT_ARCHIVIST_GATEWAY);
            format!("{}{}", gw, source)
        }
    };

    // First, check if it's a directory (for IPFS CIDs)
    if source_type == SourceType::IpfsCid {
        // Try to list directory contents
        if let Some(result) = try_import_directory(
            client,
            archivist_url,
            source,
            gateway,
            auth,
            buffer_pool,
            &mut on_progress,
        )
        .await?
        {
            return Ok(result);
        }
    }

    // Single file import
    on_progress(0.05, Some("Fetching content".to_string()), None);

    // Get content info first
    let head_response = client.head(&fetch_url).send().await?;

    if !head_response.status().is_success() {
        return Ok(JobResult::Error(format!(
            "Failed to fetch source: HTTP {}",
            head_response.status()
        )));
    }

    let content_length = head_response
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    let content_type = head_response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    if let Some(len) = content_length {
        info!(size = %format_bytes(len), "Content size detected");
    }

    // Download content using buffer pool
    let start_time = std::time::Instant::now();
    let response = client.get(&fetch_url).send().await?;

    if !response.status().is_success() {
        return Ok(JobResult::Error(format!(
            "Failed to fetch source: HTTP {}",
            response.status()
        )));
    }

    on_progress(0.1, Some("Downloading".to_string()), None);

    // Acquire buffer slot from pool (waits if pool is at capacity)
    let mut slot = buffer_pool.acquire().await;

    // Stream download into buffer (spills to temp file for large files)
    let mut stream = response.bytes_stream();
    let mut download_failed = false;

    while let Some(chunk_result) = stream.next().await {
        let chunk = match chunk_result {
            Ok(c) => c,
            Err(e) => {
                warn!(source = %source, error = %e, "Download error");
                download_failed = true;
                break;
            }
        };
        if let Err(e) = slot.write_chunk(&chunk) {
            warn!(source = %source, error = %e, "Buffer write error");
            download_failed = true;
            break;
        }
    }

    if download_failed {
        return Ok(JobResult::Error("Download failed".to_string()));
    }

    let size = slot.size();
    let download_duration = start_time.elapsed().as_secs_f64();
    let download_speed = if download_duration > 0.0 {
        size as f64 / download_duration
    } else {
        0.0
    };

    info!(
        size = %format_bytes(size),
        speed = %format_speed(download_speed),
        storage = slot.storage_type(),
        pool_memory_mb = buffer_pool.memory_used() / 1024 / 1024,
        pool_disk_mb = buffer_pool.disk_used() / 1024 / 1024,
        "Download complete"
    );

    on_progress(
        0.5,
        Some(format!("Downloaded {}", format_bytes(size))),
        Some(format_speed(download_speed)),
    );

    // Upload to Archivist - read buffer into bytes
    on_progress(0.6, Some("Uploading to Archivist".to_string()), None);

    let bytes = match slot.into_bytes() {
        Ok(b) => b,
        Err(e) => {
            return Ok(JobResult::Error(format!("Failed to read buffer: {}", e)));
        }
    };

    let upload_url = format!("{}/api/archivist/v1/data", archivist_url);
    let upload_start = std::time::Instant::now();

    let mut request = client.post(&upload_url).body(bytes);

    if let Some(ct) = &content_type {
        request = request.header("Content-Type", ct);
    }

    // Add auth headers for upload permission
    if let Some(creds) = auth {
        request = request
            .header("X-Pubkey", &creds.pubkey)
            .header("X-Timestamp", creds.timestamp.to_string())
            .header("X-Signature", &creds.signature);
    }

    let upload_response = match request.send().await {
        Ok(resp) => resp,
        Err(e) => {
            let error_msg = if e.is_connect() {
                format!("Connection failed - Archivist may be down: {}", e)
            } else if e.is_timeout() {
                format!("Request timed out: {}", e)
            } else {
                format!("Request failed: {}", e)
            };
            return Ok(JobResult::Error(format!("Failed to upload to Archivist: {}", error_msg)));
        }
    };

    if !upload_response.status().is_success() {
        let status = upload_response.status();
        let error_body = upload_response.text().await.unwrap_or_default();
        let error_msg = if error_body.is_empty() {
            format!("HTTP {}: {}", status.as_u16(), status.canonical_reason().unwrap_or("Unknown error"))
        } else {
            format!("HTTP {}: {}", status.as_u16(), error_body)
        };
        return Ok(JobResult::Error(format!("Failed to upload to Archivist: {}", error_msg)));
    }

    let new_cid = upload_response.text().await?.trim().to_string();
    let upload_duration = upload_start.elapsed().as_secs_f64();
    let upload_speed = if upload_duration > 0.0 {
        size as f64 / upload_duration
    } else {
        0.0
    };

    info!(
        cid = %new_cid,
        size = %format_bytes(size),
        upload_speed = %format_speed(upload_speed),
        "Upload complete"
    );

    on_progress(
        1.0,
        Some("Import complete".to_string()),
        Some(format_speed(upload_speed)),
    );

    Ok(JobResult::SourceImport {
        source: source.to_string(),
        new_cid,
        size,
        content_type,
        thumbnail_cid: None,
        audio_quality: None,
    })
}

/// File entry from directory listing (either from API or HTML parsing).
struct FileEntry {
    name: String,
    /// For API listings, this is the actual CID. For HTML parsed, this is empty.
    hash: Option<String>,
    #[allow(dead_code)]
    size: u64,
}

/// Try to import as a directory. Returns Some(result) if it was a directory, None otherwise.
///
/// Features:
/// - Extracts metadata from audio files (artist, album, year, track info)
/// - Renames audio files to standardized format: "01 - Track Title.ext"
/// - Generates standardized directory name: "Artist - Album (Year)"
/// - Detects and processes cover art from embedded tags or directory images
async fn try_import_directory<F>(
    client: &Client,
    archivist_url: &str,
    cid: &str,
    gateway: Option<&str>,
    auth: Option<&AuthCredentials>,
    buffer_pool: &Arc<BufferPool>,
    on_progress: &mut F,
) -> anyhow::Result<Option<JobResult>>
where
    F: FnMut(f32, Option<String>, Option<String>),
{
    let gw = gateway.unwrap_or(DEFAULT_IPFS_GATEWAY);
    let mut files: Option<Vec<FileEntry>> = None;

    // Fetch directory URL and parse HTML listing
    let dir_url = format!("{}{}", gw, cid);
    debug!(url = %dir_url, "Fetching directory listing");

    if let Ok(resp) = client.get(&dir_url).send().await {
        if resp.status().is_success() {
            let content_type: Option<String> = resp.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            if let Ok(body) = resp.text().await {
                if is_html_response(content_type.as_deref(), body.as_bytes()) {
                    let html_files = parse_ipfs_html_directory(&body, cid);
                    if !html_files.is_empty() {
                        info!(count = html_files.len(), "Parsed HTML directory listing");
                        files = Some(html_files.into_iter().map(|(name, size)| FileEntry {
                            name,
                            hash: None,
                            size,
                        }).collect());
                    }
                }
            }
        }
    }

    // No directory found
    let files = match files {
        Some(f) if !f.is_empty() => f,
        _ => return Ok(None),
    };

    info!(cid = %cid, files = files.len(), "Importing directory with metadata extraction");
    on_progress(
        0.05,
        Some(format!("Analyzing {} files", files.len())),
        None,
    );

    // Separate files into audio, images, and other
    let audio_files: Vec<&FileEntry> = files.iter()
        .filter(|f| is_audio_filename(&f.name))
        .collect();
    let cover_files: Vec<&FileEntry> = files.iter()
        .filter(|f| is_cover_image(&f.name))
        .collect();

    debug!(
        audio = audio_files.len(),
        covers = cover_files.len(),
        other = files.len() - audio_files.len() - cover_files.len(),
        "Categorized files"
    );

    // Initialize metadata tracking
    let mut import_meta = ImportMetadata::default();
    let mut entries: Vec<DirectoryEntry> = Vec::new();
    let total_files = files.len();
    let mut total_size: u64 = 0;

    // First pass: Download first audio file to extract album metadata
    if let Some(first_audio) = audio_files.first() {
        on_progress(0.08, Some("Extracting metadata from first audio file".to_string()), None);

        let file_url = build_file_url(gw, cid, first_audio);
        if let Some(bytes) = download_file_bytes(client, &file_url, &first_audio.name, buffer_pool).await {
            // Extract album-level metadata
            if let Some(album_meta) = extract_audio_metadata(&bytes, &first_audio.name) {
                import_meta.artist = album_meta.artist.clone();
                import_meta.album = album_meta.album.clone();
                import_meta.year = album_meta.year;

                info!(
                    artist = ?import_meta.artist,
                    album = ?import_meta.album,
                    year = ?import_meta.year,
                    "Extracted album metadata"
                );
            }

            // Extract audio quality
            if let Some(quality) = extract_audio_quality(&bytes, &first_audio.name) {
                info!(
                    format = %quality.format,
                    bitrate = ?quality.bitrate,
                    sample_rate = ?quality.sample_rate,
                    bit_depth = ?quality.bit_depth,
                    "Extracted audio quality"
                );
                import_meta.audio_quality = Some(quality);
            }

            // Check for embedded cover art
            if let Some(cover_art) = extract_embedded_cover(&bytes, &first_audio.name) {
                debug!(
                    mime = %cover_art.mime_type,
                    size = cover_art.data.len(),
                    "Found embedded cover art"
                );

                // Process and upload cover art
                if let Some(processed) = process_cover_bytes(&cover_art.data, CoverSource::Embedded) {
                    if let Some(result) = upload_processed_cover(
                        client,
                        archivist_url,
                        &processed,
                        Some(&cover_art.data),
                        "cover",
                        auth,
                    ).await {
                        import_meta.cover_cid = Some(result.main_cid);
                        import_meta.cover_fallback_cid = Some(result.fallback_cid);
                        info!("Uploaded embedded cover art");
                    }
                }
            }
        }
    }

    // If no embedded cover, check for cover images in directory
    if import_meta.cover_cid.is_none() && !cover_files.is_empty() {
        // Sort by cover score and pick best candidate
        let mut scored_covers: Vec<_> = cover_files.iter()
            .map(|f| (f, super::metadata::score_cover_filename(&f.name)))
            .collect();
        scored_covers.sort_by(|a, b| b.1.cmp(&a.1));

        if let Some((best_cover, score)) = scored_covers.first() {
            debug!(file = %best_cover.name, score = score, "Selected cover image from directory");

            let file_url = build_file_url(gw, cid, best_cover);
            if let Some(bytes) = download_file_bytes(client, &file_url, &best_cover.name, buffer_pool).await {
                if let Some(processed) = process_cover_bytes(&bytes, CoverSource::DirectoryFile) {
                    if let Some(result) = upload_processed_cover(
                        client,
                        archivist_url,
                        &processed,
                        Some(&bytes),
                        "cover",
                        auth,
                    ).await {
                        import_meta.cover_cid = Some(result.main_cid);
                        import_meta.cover_fallback_cid = Some(result.fallback_cid);
                        info!(file = %best_cover.name, "Uploaded directory cover art");
                    }
                }
            }
        }
    }

    // Second pass: Download all files, extract track metadata, and apply standardized naming
    for (i, file) in files.iter().enumerate() {
        let file_progress = 0.15 + (0.75 * (i as f32 / total_files as f32));
        on_progress(
            file_progress,
            Some(format!("Processing {}", &file.name)),
            None,
        );

        let file_url = build_file_url(gw, cid, file);
        debug!(file = %file.name, url = %file_url, "Downloading file");

        let response = match client.get(&file_url).send().await {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                warn!(file = %file.name, status = %r.status(), "Failed to download, skipping");
                continue;
            }
            Err(e) => {
                warn!(file = %file.name, error = %e, "Failed to download, skipping");
                continue;
            }
        };

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .or_else(|| mime_from_name(&file.name));

        // Acquire buffer slot and download
        let mut slot = buffer_pool.acquire().await;
        let mut stream = response.bytes_stream();
        let mut download_failed = false;

        while let Some(chunk_result) = stream.next().await {
            let chunk = match chunk_result {
                Ok(c) => c,
                Err(e) => {
                    warn!(file = %file.name, error = %e, "Download error, skipping");
                    download_failed = true;
                    break;
                }
            };
            if let Err(e) = slot.write_chunk(&chunk) {
                warn!(file = %file.name, error = %e, "Buffer write error, skipping");
                download_failed = true;
                break;
            }
        }

        if download_failed {
            continue;
        }

        let size = slot.size();
        total_size += size;

        let bytes = match slot.into_bytes() {
            Ok(b) => b,
            Err(e) => {
                warn!(file = %file.name, error = %e, "Failed to read buffer, skipping");
                continue;
            }
        };

        // Determine the output filename
        let output_name = if is_audio_filename(&file.name) {
            // Extract track metadata and generate standardized filename
            let track_meta = extract_track_metadata(&bytes, &file.name);

            if let Some(ref meta) = track_meta {
                let title = meta.title.as_deref().unwrap_or(&file.name);
                generate_golden_filename(meta.number, title, &file.name)
            } else {
                // No metadata - keep original name
                file.name.clone()
            }
        } else {
            // Non-audio files keep original names
            file.name.clone()
        };

        debug!(
            original = %file.name,
            output = %output_name,
            "Filename mapping"
        );

        // Upload to Archivist with the (possibly renamed) filename
        let upload_url = format!("{}/api/archivist/v1/data", archivist_url);

        let mut request = client
            .post(&upload_url)
            .header(
                "Content-Disposition",
                format!("attachment; filename=\"{}\"", output_name),
            )
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
                let error_msg = if e.is_connect() {
                    format!("Connection failed - Archivist may be down: {}", e)
                } else if e.is_timeout() {
                    format!("Request timed out: {}", e)
                } else {
                    format!("Request failed: {}", e)
                };
                warn!(file = %file.name, error = %error_msg, "Failed to upload, skipping");
                continue;
            }
        };

        if !upload_response.status().is_success() {
            let status = upload_response.status();
            let error_body = upload_response.text().await.unwrap_or_default();
            let error_msg = if error_body.is_empty() {
                format!("HTTP {}: {}", status.as_u16(), status.canonical_reason().unwrap_or("Unknown error"))
            } else {
                format!("HTTP {}: {}", status.as_u16(), error_body)
            };
            warn!(file = %file.name, error = %error_msg, "Failed to upload, skipping");
            continue;
        }

        let file_cid = upload_response.text().await?.trim().to_string();

        debug!(file = %output_name, cid = %file_cid, "Uploaded to Archivist");

        entries.push(DirectoryEntry {
            path: output_name,
            cid: file_cid,
            size,
            mimetype: content_type,
        });
    }

    if entries.is_empty() {
        return Ok(Some(JobResult::Error(
            "Failed to upload any files from directory".to_string(),
        )));
    }

    // Create directory manifest
    on_progress(0.92, Some("Creating directory manifest".to_string()), None);

    let directory_url = format!("{}/api/archivist/v1/directory", archivist_url);

    // Generate standardized directory name if we have metadata
    let directory_name = if import_meta.album.is_some() {
        Some(generate_golden_dirname(
            import_meta.artist.as_deref(),
            import_meta.album.as_deref().unwrap_or("Unknown Album"),
            import_meta.year,
        ))
    } else {
        None
    };

    let directory_request = if let Some(ref name) = directory_name {
        info!(name = %name, "Using standardized directory name");
        serde_json::json!({
            "entries": entries,
            "name": name,
        })
    } else {
        serde_json::json!({
            "entries": entries,
        })
    };

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
            return Ok(Some(JobResult::Error(format!("Failed to create directory manifest: {}", error_msg))));
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
        return Ok(Some(JobResult::Error(format!("Failed to create directory manifest: {}", error_msg))));
    }

    let directory: DirectoryResponse = directory_response.json().await?;

    info!(
        cid = %directory.cid,
        files = entries.len(),
        size = %format_bytes(total_size),
        artist = ?import_meta.artist,
        album = ?import_meta.album,
        has_cover = import_meta.cover_cid.is_some(),
        "Directory import complete"
    );

    on_progress(1.0, Some("Import complete".to_string()), None);

    Ok(Some(JobResult::SourceImport {
        source: cid.to_string(),
        new_cid: directory.cid,
        size: total_size,
        content_type: Some("inode/directory".to_string()),
        thumbnail_cid: import_meta.cover_cid,
        audio_quality: import_meta.audio_quality,
    }))
}

/// Build URL for downloading a file from the directory.
fn build_file_url(gateway: &str, cid: &str, file: &FileEntry) -> String {
    if let Some(ref hash) = file.hash {
        format!("{}{}", gateway, hash)
    } else {
        format!("{}{}/{}", gateway, cid, urlencoding::encode(&file.name))
    }
}

/// Download a file and return its bytes. Returns None on failure.
async fn download_file_bytes(
    client: &Client,
    url: &str,
    filename: &str,
    buffer_pool: &Arc<BufferPool>,
) -> Option<Vec<u8>> {
    let response = match client.get(url).send().await {
        Ok(r) if r.status().is_success() => r,
        Ok(r) => {
            warn!(file = %filename, status = %r.status(), "Failed to download");
            return None;
        }
        Err(e) => {
            warn!(file = %filename, error = %e, "Failed to download");
            return None;
        }
    };

    let mut slot = buffer_pool.acquire().await;
    let mut stream = response.bytes_stream();

    while let Some(chunk_result) = stream.next().await {
        let chunk = match chunk_result {
            Ok(c) => c,
            Err(e) => {
                warn!(file = %filename, error = %e, "Download error");
                return None;
            }
        };
        if let Err(e) = slot.write_chunk(&chunk) {
            warn!(file = %filename, error = %e, "Buffer write error");
            return None;
        }
    }

    slot.into_bytes().ok().map(|b| b.to_vec())
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
    } else if name.ends_with(".mp4") || name.ends_with(".m4v") {
        Some("video/mp4".to_string())
    } else if name.ends_with(".webm") {
        Some("video/webm".to_string())
    } else if name.ends_with(".mkv") {
        Some("video/x-matroska".to_string())
    } else if name.ends_with(".jpg") || name.ends_with(".jpeg") {
        Some("image/jpeg".to_string())
    } else if name.ends_with(".png") {
        Some("image/png".to_string())
    } else if name.ends_with(".gif") {
        Some("image/gif".to_string())
    } else if name.ends_with(".webp") {
        Some("image/webp".to_string())
    } else if name.ends_with(".pdf") {
        Some("application/pdf".to_string())
    } else if name.ends_with(".zip") {
        Some("application/zip".to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_type_detect() {
        assert_eq!(
            SourceType::detect("http://example.com/file.mp3"),
            Some(SourceType::Url)
        );
        assert_eq!(
            SourceType::detect("https://example.com/file.mp3"),
            Some(SourceType::Url)
        );
        assert_eq!(
            SourceType::detect("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"),
            Some(SourceType::IpfsCid)
        );
        assert_eq!(
            SourceType::detect("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"),
            Some(SourceType::IpfsCid)
        );
        assert_eq!(SourceType::detect("zDtest123"), Some(SourceType::ArchivistCid));
        assert_eq!(SourceType::detect("invalid"), None);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.00 KiB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MiB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GiB");
        assert_eq!(format_bytes(1536 * 1024), "1.50 MiB");
    }

    #[test]
    fn test_format_speed() {
        assert_eq!(format_speed(500.0), "500 B/s");
        assert_eq!(format_speed(1024.0), "1.00 KiB/s");
        assert_eq!(format_speed(1024.0 * 1024.0), "1.00 MiB/s");
        assert_eq!(format_speed(1024.0 * 1024.0 * 1024.0), "1.00 GiB/s");
    }

    #[test]
    fn test_mime_from_name() {
        assert_eq!(mime_from_name("track.mp3"), Some("audio/mpeg".to_string()));
        assert_eq!(mime_from_name("video.mp4"), Some("video/mp4".to_string()));
        assert_eq!(mime_from_name("image.jpg"), Some("image/jpeg".to_string()));
        assert_eq!(mime_from_name("unknown.xyz"), None);
    }

    #[test]
    fn test_parse_ipfs_html_directory() {
        // Sample HTML from actual IPFS gateway (cdn.riff.cc)
        let html = r#"
        <div>
            <a href="/ipfs/QmeyPnKAnnhhLbmPtSMu1uzDsxZjywHucyzztzpAWyv7wX/02%20-%20Go%20to%20Hell.mp3">02 - Go to Hell.mp3</a>
        </div>
        <div>
            <a class="ipfs-hash" href="/ipfs/QmUKrPaJN8XsSZZ199GvPe1h89c7sARZsNDRxaqfSTM1ii">QmUK…M1ii</a>
        </div>
        <div>
            <a href="/ipfs/QmeyPnKAnnhhLbmPtSMu1uzDsxZjywHucyzztzpAWyv7wX/03%20-%20Checkmate.mp3">03 - Checkmate.mp3</a>
        </div>
        <div>
            <a href="https://ipfs.tech">IPFS Tech</a>
        </div>
        <div>
            <a href="data:image/x-icon;base64,AAA...">favicon</a>
        </div>
        <div>
            <a href="/ipfs/QmeyPnKAnnhhLbmPtSMu1uzDsxZjywHucyzztzpAWyv7wX/%5Bcover%5D%20Loudog%20-%20Kito.jpg">[cover] Loudog - Kito.jpg</a>
        </div>
        "#;

        let base_cid = "QmeyPnKAnnhhLbmPtSMu1uzDsxZjywHucyzztzpAWyv7wX";
        let files = parse_ipfs_html_directory(html, base_cid);

        // Should find exactly 3 files in the directory (2 mp3s + 1 jpg)
        assert_eq!(files.len(), 3);

        // Check that we got the right files (decoded)
        let filenames: Vec<&str> = files.iter().map(|(name, _)| name.as_str()).collect();
        assert!(filenames.contains(&"02 - Go to Hell.mp3"));
        assert!(filenames.contains(&"03 - Checkmate.mp3"));
        assert!(filenames.contains(&"[cover] Loudog - Kito.jpg"));

        // Should NOT contain garbage
        assert!(!filenames.iter().any(|f| f.contains("ipfs.tech")));
        assert!(!filenames.iter().any(|f| f.contains("data:")));
        assert!(!filenames.iter().any(|f| f.contains("QmUK")));
    }

    #[test]
    fn test_parse_ipfs_html_directory_empty_on_non_matching() {
        // HTML with no matching links (use ## for raw string to allow # in content)
        let html = r##"
        <a href="https://example.com/file.mp3">External</a>
        <a href="/ipfs/OtherCid/file.mp3">Different CID</a>
        <a href="#anchor">Anchor</a>
        "##;

        let files = parse_ipfs_html_directory(html, "QmTargetCid");
        assert!(files.is_empty());
    }
}
