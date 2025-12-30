//! Analyze executor for metadata extraction.
//!
//! Analyzes content that is already on Archivist/IPFS to extract:
//! - Audio quality (format, bitrate, codec)
//! - Track metadata (titles, track numbers)
//!
//! Does NOT re-upload content - just fetches headers and updates Citadel Lens.
//! This is efficient: we only download the first few KB of each file to detect format.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::crdt::{AudioQuality, JobResult};
use super::source::{SourceType, DEFAULT_ARCHIVIST_GATEWAY, DEFAULT_IPFS_GATEWAY};

/// Track metadata extracted from analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackInfo {
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artist: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track_number: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<f64>,
}

/// Archivist directory listing response.
#[derive(Debug, Deserialize)]
struct ArchivistDirectory {
    entries: Vec<ArchivistEntry>,
}

#[derive(Debug, Deserialize)]
struct ArchivistEntry {
    path: String,
    cid: String,
    size: u64,
    #[serde(default)]
    mimetype: Option<String>,
}

/// Progress callback type.
pub type ProgressCallback = Box<dyn FnMut(f32, Option<String>) + Send>;

/// Parse IPFS gateway HTML directory listing to extract file links.
///
/// The HTML structure for file entries is:
/// ```html
/// <a href="/ipfs/{base_cid}/{encoded_filename}">{filename}</a>
/// ```
///
/// We need to match only links that start with `/ipfs/{base_cid}/` to avoid
/// picking up external links, navigation, data URIs, or individual file CID links.
fn parse_ipfs_html_directory(html: &str, base_cid: &str) -> Vec<(String, String, u64)> {
    let mut files = Vec::new();

    // Build the prefix pattern for this directory's file links
    let dir_prefix = format!("/ipfs/{}/", base_cid);

    // Simple parsing for <a href="...">text</a>
    let mut pos = 0;
    while let Some(href_start) = html[pos..].find("href=\"") {
        let href_start = pos + href_start + 6; // Skip 'href="'
        if let Some(href_end) = html[href_start..].find('"') {
            let href = &html[href_start..href_start + href_end];

            // Only process links that are within this directory
            if let Some(rest) = href.strip_prefix(&dir_prefix) {
                // Extract just the filename part (before any query params)
                let filename = rest.split('?').next().unwrap_or(rest);

                // URL decode the filename
                let filename = urlencoding::decode(filename)
                    .map(|s| s.into_owned())
                    .unwrap_or_else(|_| filename.to_string());

                // Skip empty filenames and navigation, only include audio files
                if !filename.is_empty()
                    && filename != ".."
                    && filename != "."
                    && !filename.starts_with('#')
                    && is_audio_file(&filename)
                {
                    // Deduplicate - only add if not already present
                    if !files.iter().any(|(name, _, _)| name == &filename) {
                        // For IPFS directories, we construct file URL from base CID + filename
                        files.push((filename.clone(), format!("{}/{}", base_cid, filename), 0));
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

/// Check if response looks like HTML (directory listing page).
fn is_html_response(content_type: Option<&str>, body_preview: &[u8]) -> bool {
    // Check content-type header
    if let Some(ct) = content_type {
        if ct.contains("text/html") {
            return true;
        }
    }
    // Check for HTML markers in body
    if body_preview.len() >= 15 {
        let preview = String::from_utf8_lossy(&body_preview[..body_preview.len().min(500)]);
        return preview.contains("<!DOCTYPE")
            || preview.contains("<html")
            || preview.contains("<HTML")
            || preview.contains("Index of /ipfs");
    }
    false
}

/// Audio file extensions we recognize.
const AUDIO_EXTENSIONS: &[&str] = &[
    ".flac", ".mp3", ".ogg", ".opus", ".m4a", ".aac", ".wav", ".aiff", ".wma",
];

/// Check if filename is an audio file.
fn is_audio_file(name: &str) -> bool {
    let lower = name.to_lowercase();
    AUDIO_EXTENSIONS.iter().any(|ext| lower.ends_with(ext))
}

/// Detect audio format from filename extension.
fn format_from_extension(name: &str) -> Option<String> {
    let lower = name.to_lowercase();
    if lower.ends_with(".flac") {
        Some("flac".to_string())
    } else if lower.ends_with(".mp3") {
        Some("mp3".to_string())
    } else if lower.ends_with(".ogg") {
        Some("ogg".to_string())
    } else if lower.ends_with(".opus") {
        Some("opus".to_string())
    } else if lower.ends_with(".m4a") || lower.ends_with(".aac") {
        Some("aac".to_string())
    } else if lower.ends_with(".wav") {
        Some("wav".to_string())
    } else if lower.ends_with(".aiff") {
        Some("aiff".to_string())
    } else if lower.ends_with(".wma") {
        Some("wma".to_string())
    } else {
        None
    }
}

/// Extract track title from filename.
/// Tries to parse "01 - Track Name.flac" or "01. Track Name.mp3" patterns.
fn extract_track_info(filename: &str) -> TrackInfo {
    // Remove extension
    let name = if let Some(dot_pos) = filename.rfind('.') {
        &filename[..dot_pos]
    } else {
        filename
    };

    // Try to parse track number patterns
    // "01 - Track Name" or "01. Track Name" or "01-Track Name" or "1. Track Name"
    let (track_number, title) = if let Some(cap) = parse_track_pattern(name) {
        cap
    } else {
        (None, name.to_string())
    };

    TrackInfo {
        title: title.trim().to_string(),
        artist: None,
        track_number,
        duration: None,
    }
}

/// Parse common track number patterns.
fn parse_track_pattern(name: &str) -> Option<(Option<u32>, String)> {
    // Pattern: "01 - Title" or "1 - Title"
    if let Some(idx) = name.find(" - ") {
        let prefix = &name[..idx];
        if let Ok(num) = prefix.trim().parse::<u32>() {
            return Some((Some(num), name[idx + 3..].to_string()));
        }
    }

    // Pattern: "01. Title" or "1. Title"
    if let Some(idx) = name.find(". ") {
        let prefix = &name[..idx];
        if let Ok(num) = prefix.trim().parse::<u32>() {
            return Some((Some(num), name[idx + 2..].to_string()));
        }
    }

    // Pattern: "01-Title" (no spaces)
    if let Some(idx) = name.find('-') {
        let prefix = &name[..idx];
        if let Ok(num) = prefix.trim().parse::<u32>() {
            return Some((Some(num), name[idx + 1..].to_string()));
        }
    }

    // Pattern: starts with two digits "01Title"
    if name.len() >= 2 {
        if let Ok(num) = name[..2].parse::<u32>() {
            let rest = name[2..].trim_start_matches(|c: char| c == ' ' || c == '-' || c == '.');
            if !rest.is_empty() {
                return Some((Some(num), rest.to_string()));
            }
        }
    }

    None
}

/// Detect audio quality from file header bytes.
/// This function analyzes the first bytes of an audio file to determine format and quality.
fn detect_quality_from_header(header: &[u8], filename: &str) -> Option<AudioQuality> {
    // Check FLAC magic: "fLaC"
    if header.len() >= 4 && &header[0..4] == b"fLaC" {
        // FLAC - try to parse STREAMINFO block for bit depth
        if header.len() >= 22 {
            // STREAMINFO is at offset 4, block header is 4 bytes
            // Bits per sample is at offset 18, bits 4-8 (5 bits)
            let info_start = 8; // Skip "fLaC" + block header
            if header.len() >= info_start + 14 {
                // Sample rate is 20 bits at offset 10
                let sample_rate_high = (header[info_start + 10] as u32) << 12;
                let sample_rate_mid = (header[info_start + 11] as u32) << 4;
                let sample_rate_low = (header[info_start + 12] as u32) >> 4;
                let sample_rate = sample_rate_high | sample_rate_mid | sample_rate_low;

                // Bits per sample is 5 bits starting at bit 4 of offset 12
                let bits_byte = ((header[info_start + 12] & 0x01) << 4) | ((header[info_start + 13] >> 4) & 0x0F);
                let bit_depth = bits_byte as u32 + 1;

                return Some(AudioQuality {
                    format: "flac".to_string(),
                    bitrate: None,
                    sample_rate: if sample_rate > 0 { Some(sample_rate) } else { Some(44100) },
                    bit_depth: if bit_depth > 0 && bit_depth <= 32 { Some(bit_depth) } else { Some(16) },
                    codec: Some("FLAC".to_string()),
                });
            }
        }
        return Some(AudioQuality::flac());
    }

    // Check MP3: ID3 tag or frame sync
    if header.len() >= 3 {
        let is_mp3 = (header[0..3] == *b"ID3") // ID3v2 tag
            || (header[0] == 0xFF && (header[1] & 0xE0) == 0xE0); // Frame sync

        if is_mp3 {
            // Try to find frame sync and extract bitrate
            let bitrate = extract_mp3_bitrate(header).unwrap_or(320);
            return Some(AudioQuality::mp3(bitrate));
        }
    }

    // Check OGG: "OggS"
    if header.len() >= 4 && &header[0..4] == b"OggS" {
        // Check if it's Opus or Vorbis by looking for signature in first page
        if header.len() >= 35 {
            // Opus: "OpusHead" at offset 28
            if header.len() >= 36 && &header[28..36] == b"OpusHead" {
                return Some(AudioQuality {
                    format: "opus".to_string(),
                    bitrate: Some(128), // Default guess for Opus
                    sample_rate: Some(48000), // Opus is always 48kHz internally
                    bit_depth: None,
                    codec: Some("Opus".to_string()),
                });
            }
            // Vorbis: "\x01vorbis" in header
            for i in 0..header.len().saturating_sub(7) {
                if header[i] == 0x01 && &header[i + 1..i + 7] == b"vorbis" {
                    return Some(AudioQuality {
                        format: "ogg".to_string(),
                        bitrate: Some(192), // Default guess
                        sample_rate: Some(44100),
                        bit_depth: None,
                        codec: Some("Vorbis".to_string()),
                    });
                }
            }
        }
        // Generic Ogg
        return Some(AudioQuality {
            format: "ogg".to_string(),
            bitrate: None,
            sample_rate: None,
            bit_depth: None,
            codec: None,
        });
    }

    // Check WAV: "RIFF....WAVE"
    if header.len() >= 12 && &header[0..4] == b"RIFF" && &header[8..12] == b"WAVE" {
        // Parse fmt chunk for bit depth and sample rate
        if header.len() >= 44 {
            // fmt chunk usually starts at offset 12
            if &header[12..16] == b"fmt " {
                // Sample rate at offset 24 (4 bytes, little endian)
                let sample_rate = u32::from_le_bytes([header[24], header[25], header[26], header[27]]);
                // Bits per sample at offset 34 (2 bytes, little endian)
                let bit_depth = u16::from_le_bytes([header[34], header[35]]) as u32;

                return Some(AudioQuality {
                    format: "wav".to_string(),
                    bitrate: None,
                    sample_rate: Some(sample_rate),
                    bit_depth: Some(bit_depth),
                    codec: Some("PCM".to_string()),
                });
            }
        }
        return Some(AudioQuality {
            format: "wav".to_string(),
            bitrate: None,
            sample_rate: Some(44100),
            bit_depth: Some(16),
            codec: Some("PCM".to_string()),
        });
    }

    // Check AAC/M4A: "....ftyp" or "....moov"
    if header.len() >= 8 {
        if &header[4..8] == b"ftyp" || &header[4..8] == b"moov" {
            return Some(AudioQuality {
                format: "aac".to_string(),
                bitrate: Some(256), // Default guess
                sample_rate: Some(44100),
                bit_depth: None,
                codec: Some("AAC".to_string()),
            });
        }
    }

    // Fallback to extension-based detection
    format_from_extension(filename).map(|format| {
        let (bitrate, sample_rate, bit_depth, codec) = match format.as_str() {
            "flac" => (None, Some(44100), Some(16), Some("FLAC".to_string())),
            "mp3" => (Some(320), None, None, Some("MP3".to_string())),
            "ogg" => (Some(192), Some(44100), None, Some("Vorbis".to_string())),
            "opus" => (Some(128), Some(48000), None, Some("Opus".to_string())),
            "aac" | "m4a" => (Some(256), Some(44100), None, Some("AAC".to_string())),
            "wav" => (None, Some(44100), Some(16), Some("PCM".to_string())),
            _ => (None, None, None, None),
        };
        AudioQuality {
            format,
            bitrate,
            sample_rate,
            bit_depth,
            codec,
        }
    })
}

/// Extract MP3 bitrate from header.
fn extract_mp3_bitrate(header: &[u8]) -> Option<u32> {
    // Find frame sync (0xFF followed by 0xE0-0xFF)
    for i in 0..header.len().saturating_sub(4) {
        if header[i] == 0xFF && (header[i + 1] & 0xE0) == 0xE0 {
            // Found frame sync
            let version = (header[i + 1] >> 3) & 0x03;
            let layer = (header[i + 1] >> 1) & 0x03;
            let bitrate_index = (header[i + 2] >> 4) & 0x0F;

            // Only handle MPEG-1 Layer 3 (most common)
            if version == 0x03 && layer == 0x01 && bitrate_index > 0 && bitrate_index < 15 {
                const BITRATES: [u32; 15] = [
                    0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320,
                ];
                return Some(BITRATES[bitrate_index as usize]);
            }
        }
    }
    None
}

/// Execute an analyze job.
///
/// Fetches content metadata from Archivist/IPFS (without full download)
/// and extracts audio quality and track information.
pub async fn execute_analyze<F>(
    client: &Client,
    content_cid: &str,
    release_id: &str,
    gateway: Option<&str>,
    lens_url: Option<&str>,
    mut on_progress: F,
) -> anyhow::Result<JobResult>
where
    F: FnMut(f32, Option<String>),
{
    let source_type = SourceType::detect(content_cid)
        .ok_or_else(|| anyhow::anyhow!("Invalid CID format: {}", content_cid))?;

    info!(cid = %content_cid, release_id = %release_id, "Starting content analysis");
    on_progress(0.0, Some("Starting analysis".to_string()));

    // Build base URL for fetching content
    let base_url = match source_type {
        SourceType::ArchivistCid => {
            let gw = gateway.unwrap_or(DEFAULT_ARCHIVIST_GATEWAY);
            format!("{}{}", gw, content_cid)
        }
        SourceType::IpfsCid => {
            let gw = gateway.unwrap_or(DEFAULT_IPFS_GATEWAY);
            format!("{}{}", gw, content_cid)
        }
        SourceType::Url => content_cid.to_string(),
    };

    on_progress(0.1, Some("Fetching directory listing".to_string()));

    // Try to get directory listing
    let audio_files = if source_type == SourceType::ArchivistCid {
        // Archivist directory API
        let dir_url = format!("{}/directory", base_url.trim_end_matches('/'));
        debug!(url = %dir_url, "Fetching Archivist directory");

        match client.get(&dir_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let dir: ArchivistDirectory = resp.json().await?;
                dir.entries
                    .into_iter()
                    .filter(|e| is_audio_file(&e.path))
                    .map(|e| (e.path, e.cid, e.size))
                    .collect::<Vec<_>>()
            }
            _ => {
                // Single file, not a directory
                vec![(content_cid.to_string(), content_cid.to_string(), 0)]
            }
        }
    } else {
        // For IPFS, fetch directory URL and parse HTML listing
        // Most IPFS gateways (including ours) only serve HTML directory pages, not the IPFS API
        let mut files_found = None;

        debug!(url = %base_url, "Fetching directory listing");
        if let Ok(resp) = client.get(&base_url).send().await {
            if resp.status().is_success() {
                let content_type: Option<String> = resp.headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());

                if let Ok(body) = resp.text().await {
                    if is_html_response(content_type.as_deref(), body.as_bytes()) {
                        let files = parse_ipfs_html_directory(&body, content_cid);
                        if !files.is_empty() {
                            info!(count = files.len(), "Parsed HTML directory listing");
                            files_found = Some(files);
                        }
                    }
                }
            }
        }

        // Fallback: treat as single file
        files_found.unwrap_or_else(|| {
            debug!(cid = %content_cid, "Treating as single file");
            vec![(content_cid.to_string(), content_cid.to_string(), 0)]
        })
    };

    if audio_files.is_empty() {
        warn!(cid = %content_cid, "No audio files found in content");
        return Ok(JobResult::Analyze {
            release_id: release_id.to_string(),
            audio_quality: None,
            tracks_found: 0,
            track_metadata: None,
        });
    }

    info!(cid = %content_cid, files = audio_files.len(), "Found audio files");
    on_progress(0.2, Some(format!("Analyzing {} audio files", audio_files.len())));

    // Analyze each file's header for quality detection
    let mut tracks: Vec<TrackInfo> = Vec::new();
    let mut primary_quality: Option<AudioQuality> = None;
    let total_files = audio_files.len();

    for (i, (filename, file_cid, _size)) in audio_files.iter().enumerate() {
        let progress = 0.2 + (0.6 * (i as f32 / total_files as f32));
        on_progress(progress, Some(format!("Analyzing: {}", filename)));

        // Fetch just the first 1KB of the file to detect format
        let file_url = match source_type {
            SourceType::ArchivistCid => {
                let gw = gateway.unwrap_or(DEFAULT_ARCHIVIST_GATEWAY);
                format!("{}{}", gw, file_cid)
            }
            SourceType::IpfsCid => {
                let gw = gateway.unwrap_or(DEFAULT_IPFS_GATEWAY);
                format!("{}{}", gw, file_cid)
            }
            SourceType::Url => file_cid.to_string(),
        };

        // Use Range request to fetch just the header
        let header_bytes = match client
            .get(&file_url)
            .header("Range", "bytes=0-1023")
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() || resp.status().as_u16() == 206 => {
                resp.bytes().await.ok()
            }
            _ => None,
        };

        // Extract track info from filename
        let mut track_info = extract_track_info(filename);
        if track_info.track_number.is_none() {
            track_info.track_number = Some((i + 1) as u32);
        }
        tracks.push(track_info);

        // Detect quality from header
        if let Some(header) = header_bytes {
            if let Some(quality) = detect_quality_from_header(&header, filename) {
                debug!(
                    file = %filename,
                    format = %quality.format,
                    bitrate = ?quality.bitrate,
                    "Detected audio quality"
                );

                // Use first detected quality as primary
                if primary_quality.is_none() {
                    primary_quality = Some(quality);
                }
            }
        }
    }

    // Sort tracks by track number
    tracks.sort_by_key(|t| t.track_number.unwrap_or(999));

    let track_metadata = serde_json::to_string(&tracks).ok();
    let tracks_found = tracks.len();

    info!(
        cid = %content_cid,
        tracks = tracks_found,
        quality = ?primary_quality.as_ref().map(|q| &q.format),
        "Analysis complete"
    );

    on_progress(0.85, Some("Updating Citadel Lens".to_string()));

    // Update release in Citadel Lens
    if let Some(lens) = lens_url {
        if let Some(ref quality) = primary_quality {
            let update_url = format!(
                "{}/api/v1/releases/{}/metadata",
                lens.trim_end_matches('/'),
                release_id
            );

            let update_payload = serde_json::json!({
                "audioQuality": quality,
                "trackMetadata": track_metadata,
            });

            match client
                .patch(&update_url)
                .json(&update_payload)
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    info!(release_id = %release_id, "Updated release metadata in Citadel Lens");
                }
                Ok(resp) => {
                    warn!(
                        release_id = %release_id,
                        status = %resp.status(),
                        "Failed to update release in Citadel Lens"
                    );
                }
                Err(e) => {
                    warn!(
                        release_id = %release_id,
                        error = %e,
                        "Failed to update release in Citadel Lens"
                    );
                }
            }
        }
    }

    on_progress(1.0, Some("Analysis complete".to_string()));

    Ok(JobResult::Analyze {
        release_id: release_id.to_string(),
        audio_quality: primary_quality,
        tracks_found,
        track_metadata,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_audio_file() {
        assert!(is_audio_file("track.mp3"));
        assert!(is_audio_file("song.FLAC"));
        assert!(is_audio_file("audio.opus"));
        assert!(!is_audio_file("image.jpg"));
        assert!(!is_audio_file("readme.txt"));
    }

    #[test]
    fn test_extract_track_info() {
        let info = extract_track_info("01 - Track Name.flac");
        assert_eq!(info.track_number, Some(1));
        assert_eq!(info.title, "Track Name");

        let info = extract_track_info("12. Another Song.mp3");
        assert_eq!(info.track_number, Some(12));
        assert_eq!(info.title, "Another Song");

        let info = extract_track_info("3-Title.ogg");
        assert_eq!(info.track_number, Some(3));
        assert_eq!(info.title, "Title");

        let info = extract_track_info("Just A Name.wav");
        assert_eq!(info.track_number, None);
        assert_eq!(info.title, "Just A Name");
    }

    #[test]
    fn test_detect_flac_header() {
        let header = b"fLaC\x00\x00\x00\x22\x10\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\xAC\x44\x10\x00";
        let quality = detect_quality_from_header(header, "test.flac");
        assert!(quality.is_some());
        let q = quality.unwrap();
        assert_eq!(q.format, "flac");
    }

    #[test]
    fn test_detect_mp3_header() {
        let header = b"ID3\x04\x00\x00\x00\x00\x00\x00";
        let quality = detect_quality_from_header(header, "test.mp3");
        assert!(quality.is_some());
        let q = quality.unwrap();
        assert_eq!(q.format, "mp3");
    }

    #[test]
    fn test_detect_ogg_header() {
        let header = b"OggS\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00";
        let quality = detect_quality_from_header(header, "test.ogg");
        assert!(quality.is_some());
        let q = quality.unwrap();
        assert_eq!(q.format, "ogg");
    }
}
