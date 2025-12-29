//! Source import executor for URLs and CIDs.
//!
//! Downloads content from URLs, IPFS gateways, or Archivist and re-uploads
//! to the target Archivist instance. Supports single files and directories.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::crdt::JobResult;

/// Default IPFS gateway for CID resolution.
pub const DEFAULT_IPFS_GATEWAY: &str = "https://cdn.riff.cc/ipfs/";

/// Default Archivist gateway for CID resolution.
pub const DEFAULT_ARCHIVIST_GATEWAY: &str = "https://archivist.riff.cc/api/archivist/v1/data/";

/// Maximum size for in-memory download (10 GiB).
pub const MAX_IN_MEMORY_SIZE: u64 = 10 * 1024 * 1024 * 1024;

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

/// IPFS directory listing from gateway.
#[derive(Debug, Deserialize)]
struct IpfsDirectoryListing {
    #[serde(rename = "Objects")]
    objects: Vec<IpfsObject>,
}

#[derive(Debug, Deserialize)]
struct IpfsObject {
    #[serde(rename = "Links")]
    links: Vec<IpfsLink>,
}

#[derive(Debug, Deserialize)]
struct IpfsLink {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Hash")]
    hash: String,
    #[serde(rename = "Size")]
    size: u64,
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

/// Execute a source import job.
///
/// # Arguments
/// * `client` - HTTP client
/// * `archivist_url` - Base URL of target Archivist API
/// * `source` - Source URL or CID
/// * `gateway` - Optional gateway override for CID resolution
/// * `auth_pubkey` - Optional pre-authorized public key for Archivist uploads
/// * `on_progress` - Callback for progress updates (progress 0-1, message, speed)
pub async fn execute_source_import<F>(
    client: &Client,
    archivist_url: &str,
    source: &str,
    gateway: Option<&str>,
    auth_pubkey: Option<&str>,
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
            auth_pubkey,
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
        if len > MAX_IN_MEMORY_SIZE {
            return Ok(JobResult::Error(format!(
                "Content too large for in-memory import: {} (max {})",
                format_bytes(len),
                format_bytes(MAX_IN_MEMORY_SIZE)
            )));
        }
    }

    // Download content
    let start_time = std::time::Instant::now();
    let response = client.get(&fetch_url).send().await?;

    if !response.status().is_success() {
        return Ok(JobResult::Error(format!(
            "Failed to fetch source: HTTP {}",
            response.status()
        )));
    }

    on_progress(0.1, Some("Downloading".to_string()), None);

    let bytes = response.bytes().await?;
    let size = bytes.len() as u64;
    let download_duration = start_time.elapsed().as_secs_f64();
    let download_speed = if download_duration > 0.0 {
        size as f64 / download_duration
    } else {
        0.0
    };

    info!(
        size = %format_bytes(size),
        speed = %format_speed(download_speed),
        "Download complete"
    );

    on_progress(
        0.5,
        Some(format!("Downloaded {}", format_bytes(size))),
        Some(format_speed(download_speed)),
    );

    // Upload to Archivist
    on_progress(0.6, Some("Uploading to Archivist".to_string()), None);

    let upload_url = format!("{}/api/archivist/v1/data", archivist_url);
    let upload_start = std::time::Instant::now();

    let mut request = client.post(&upload_url).body(bytes);

    if let Some(ct) = &content_type {
        request = request.header("Content-Type", ct);
    }

    if let Some(pubkey) = auth_pubkey {
        request = request.header("X-Pubkey", pubkey);
    }

    let upload_response = request.send().await?;

    if !upload_response.status().is_success() {
        let error = upload_response.text().await?;
        return Ok(JobResult::Error(format!(
            "Failed to upload to Archivist: {}",
            error
        )));
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
    })
}

/// Try to import as a directory. Returns Some(result) if it was a directory, None otherwise.
async fn try_import_directory<F>(
    client: &Client,
    archivist_url: &str,
    cid: &str,
    gateway: Option<&str>,
    auth_pubkey: Option<&str>,
    on_progress: &mut F,
) -> anyhow::Result<Option<JobResult>>
where
    F: FnMut(f32, Option<String>, Option<String>),
{
    let gw = gateway.unwrap_or(DEFAULT_IPFS_GATEWAY);

    // Try to get directory listing via IPFS API
    // Most gateways support /api/v0/ls for directory listing
    let ls_url = format!(
        "{}api/v0/ls?arg={}",
        gw.trim_end_matches("ipfs/"),
        cid
    );

    debug!(url = %ls_url, "Trying directory listing");

    let response = match client.post(&ls_url).send().await {
        Ok(r) => r,
        Err(_) => return Ok(None), // Not a directory or listing not supported
    };

    if !response.status().is_success() {
        return Ok(None); // Not a directory
    }

    let listing: IpfsDirectoryListing = match response.json().await {
        Ok(l) => l,
        Err(_) => return Ok(None), // Not a directory listing format
    };

    let links = &listing.objects.first().map(|o| &o.links);
    let links = match links {
        Some(l) if !l.is_empty() => l,
        _ => return Ok(None), // Empty or no links = single file
    };

    info!(cid = %cid, files = links.len(), "Importing directory");
    on_progress(
        0.05,
        Some(format!("Importing {} files", links.len())),
        None,
    );

    // Upload each file individually
    let mut entries: Vec<DirectoryEntry> = Vec::new();
    let total_files = links.len();
    let mut total_size: u64 = 0;

    for (i, link) in links.iter().enumerate() {
        let file_progress = 0.05 + (0.85 * (i as f32 / total_files as f32));
        on_progress(
            file_progress,
            Some(format!("Uploading {}", &link.name)),
            None,
        );

        let file_url = format!("{}{}", gw, link.hash);

        debug!(file = %link.name, hash = %link.hash, "Downloading file");

        let response = match client.get(&file_url).send().await {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                warn!(file = %link.name, status = %r.status(), "Failed to download, skipping");
                continue;
            }
            Err(e) => {
                warn!(file = %link.name, error = %e, "Failed to download, skipping");
                continue;
            }
        };

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .or_else(|| mime_from_name(&link.name));

        let bytes = response.bytes().await?;
        let size = bytes.len() as u64;
        total_size += size;

        // Upload to Archivist
        let upload_url = format!("{}/api/archivist/v1/data", archivist_url);

        let mut request = client
            .post(&upload_url)
            .header(
                "Content-Disposition",
                format!("attachment; filename=\"{}\"", link.name),
            )
            .body(bytes);

        if let Some(ct) = &content_type {
            request = request.header("Content-Type", ct);
        }

        if let Some(pubkey) = auth_pubkey {
            request = request.header("X-Pubkey", pubkey);
        }

        let upload_response = request.send().await?;

        if !upload_response.status().is_success() {
            let error = upload_response.text().await?;
            warn!(file = %link.name, error = %error, "Failed to upload, skipping");
            continue;
        }

        let file_cid = upload_response.text().await?.trim().to_string();

        debug!(file = %link.name, cid = %file_cid, "Uploaded to Archivist");

        entries.push(DirectoryEntry {
            path: link.name.clone(),
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

    let directory_request = serde_json::json!({
        "entries": entries,
    });

    let mut request = client.post(&directory_url).json(&directory_request);

    if let Some(pubkey) = auth_pubkey {
        request = request.header("X-Pubkey", pubkey);
    }

    let directory_response = request.send().await?;

    if !directory_response.status().is_success() {
        let error = directory_response.text().await?;
        return Ok(Some(JobResult::Error(format!(
            "Failed to create directory manifest: {}",
            error
        ))));
    }

    let directory: DirectoryResponse = directory_response.json().await?;

    info!(
        cid = %directory.cid,
        files = entries.len(),
        size = %format_bytes(total_size),
        "Directory import complete"
    );

    on_progress(1.0, Some("Import complete".to_string()), None);

    Ok(Some(JobResult::SourceImport {
        source: cid.to_string(),
        new_cid: directory.cid,
        size: total_size,
        content_type: Some("inode/directory".to_string()),
    }))
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
}
