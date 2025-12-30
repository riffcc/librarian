//! Import executor for Archive.org items.
//!
//! Downloads files from Archive.org and uploads them to Archivist,
//! creating a directory manifest for the complete release.
//!
//! Downloads run in PARALLEL up to the buffer pool's max_concurrent limit.
//! This means a 10-track album with --concurrent=8 downloads 8 tracks at once.
//!
//! Extracts rich metadata from Archive.org including:
//! - Album/release title and artist
//! - Per-track titles for nice file naming ("01 - Track Name.mp3")
//! - License information for automatic categorization

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{stream, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn, debug};

use crate::auth::AuthCredentials;
use crate::crdt::{AudioQuality, JobResult};
use super::buffer::BufferPool;
use super::ArchiveRateLimiter;

/// Base delay for exponential backoff (doubles each retry, caps at MAX_RETRY_DELAY)
const RETRY_BASE_DELAY_MS: u64 = 500;

/// Maximum delay between retries (10 minutes)
const MAX_RETRY_DELAY_MS: u64 = 600_000;

/// File entry for directory manifest.
#[derive(Debug, Clone, Serialize)]
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

/// Archive.org file entry from metadata API.
#[derive(Debug, Clone, Deserialize)]
pub struct ArchiveFileEntry {
    pub name: String,
    pub format: Option<String>,
    pub size: Option<String>,
    pub source: Option<String>,
    // Rich metadata from ID3/Vorbis tags (extracted by Archive.org)
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub track: Option<String>,
    pub length: Option<String>,
    pub genre: Option<String>,
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
/// Also unescapes backslash-escaped characters from Archive.org.
fn extract_string(value: &Option<serde_json::Value>) -> Option<String> {
    let raw = match value {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Array(arr)) => arr
            .first()
            .and_then(|v| v.as_str())
            .map(String::from),
        _ => None,
    };
    raw.map(|s| unescape_archive_metadata(&s))
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
    if let Some(raw_title) = &file.title {
        // Unescape Archive.org metadata (e.g., "\(" → "(")
        let title = unescape_archive_metadata(raw_title);
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

/// Metadata file formats to include for attribution/provenance.
const METADATA_FORMATS: &[&str] = &[
    "Metadata",
    "Text",
];

/// Image formats that might contain cover art.
const IMAGE_FORMATS: &[&str] = &[
    "JPEG",
    "PNG",
    "GIF",
    "Image",
];

/// Common cover art filenames (lowercase, checked in order of priority).
/// Higher priority = more likely to be the actual cover art.
const COVER_ART_PRIORITY_NAMES: &[&str] = &[
    "cover.jpg", "cover.jpeg", "cover.png",
    "folder.jpg", "folder.jpeg", "folder.png",
    "front.jpg", "front.jpeg", "front.png",
    "album.jpg", "album.jpeg", "album.png",
    "artwork.jpg", "artwork.jpeg", "artwork.png",
];

/// Archive.org's auto-generated thumbnail filename.
const IA_THUMB_FILENAME: &str = "__ia_thumb.jpg";

/// Quality tier for audio files.
/// Each tier gets its own directory CID for the Quality Ladder.
///
/// Preferred formats (in order):
/// - Lossless (FLAC, WAV, etc.) - source of truth
/// - Opus - modern transparent lossy, what we transcode to
/// - MP3 320 CBR / V0 - legacy "perfect" lossy formats
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QualityTier {
    /// Lossless: FLAC, WAV, AIFF, ALAC (the gold standard)
    Lossless,
    /// MP3 320 CBR - "Perfect" lossy CBR
    Mp3_320,
    /// MP3 V0 - "Perfect" lossy VBR (~245kbps average, LAME -V0)
    Mp3V0,
    /// MP3 256 CBR
    Mp3_256,
    /// MP3 V1/V2 and other VBR
    Mp3Vbr,
    /// MP3 192 CBR or lower quality CBR
    Mp3_192,
    /// AAC/M4A - patent-encumbered, only use if nothing else available
    Aac,
    /// Ogg Vorbis
    OggVorbis,
    /// Opus codec
    Opus,
}

impl QualityTier {
    /// Get the tier name for API responses.
    pub fn as_str(&self) -> &'static str {
        match self {
            QualityTier::Lossless => "lossless",
            QualityTier::Mp3_320 => "mp3_320",
            QualityTier::Mp3V0 => "mp3_v0",
            QualityTier::Mp3_256 => "mp3_256",
            QualityTier::Mp3Vbr => "mp3_vbr",
            QualityTier::Mp3_192 => "mp3_192",
            QualityTier::Aac => "aac",
            QualityTier::OggVorbis => "ogg",
            QualityTier::Opus => "opus",
        }
    }

    /// Get the priority for selecting the "best" tier (higher = better).
    /// Opus is the modern gold standard for lossy - transparent at ~128kbps.
    pub fn priority(&self) -> u8 {
        match self {
            // Lossless is always the source of truth
            QualityTier::Lossless => 100,
            // Opus is THE modern lossy codec - what we transcode FLAC to
            QualityTier::Opus => 95,
            // High quality legacy lossy (don't re-encode, serve as-is)
            QualityTier::Mp3_320 => 85,
            QualityTier::Mp3V0 => 84,
            // Other decent lossy
            QualityTier::Mp3_256 => 75,
            QualityTier::OggVorbis => 73,
            QualityTier::Mp3Vbr => 65,     // V1, V2, etc.
            // Lower quality fallbacks
            QualityTier::Mp3_192 => 50,
            // Patent-encumbered formats - only if nothing else available
            QualityTier::Aac => 30,
        }
    }

    /// Check if this tier is suitable as a "primary" format.
    /// Lossless or high-quality lossy that shouldn't be transcoded further.
    pub fn is_primary_quality(&self) -> bool {
        matches!(self,
            QualityTier::Lossless |
            QualityTier::Opus |
            QualityTier::Mp3_320 |
            QualityTier::Mp3V0
        )
    }

    /// Check if this is lossless (can be transcoded to Opus ladder).
    pub fn is_lossless(&self) -> bool {
        matches!(self, QualityTier::Lossless)
    }

    /// Convert this tier to an AudioQuality struct for metadata storage.
    pub fn to_audio_quality(&self) -> AudioQuality {
        match self {
            QualityTier::Lossless => AudioQuality::flac(),
            QualityTier::Mp3_320 => AudioQuality::mp3(320),
            QualityTier::Mp3V0 => AudioQuality::mp3_vbr(),
            QualityTier::Mp3_256 => AudioQuality::mp3(256),
            QualityTier::Mp3Vbr => AudioQuality::mp3_vbr(),
            QualityTier::Mp3_192 => AudioQuality::mp3(192),
            QualityTier::Aac => AudioQuality::aac(),
            QualityTier::OggVorbis => AudioQuality::vorbis(),
            QualityTier::Opus => AudioQuality::opus(None),
        }
    }
}

/// Detect quality tier from Archive.org file metadata.
fn detect_quality_tier(file: &ArchiveFileEntry) -> Option<QualityTier> {
    let format = file.format.as_deref().unwrap_or("");
    let name = file.name.to_lowercase();

    // Skip files we don't want
    if should_skip_file(file) {
        return None;
    }

    // Lossless formats
    if format.contains("FLAC") || name.ends_with(".flac") {
        return Some(QualityTier::Lossless);
    }
    if format.contains("WAV") || name.ends_with(".wav") {
        return Some(QualityTier::Lossless);
    }
    if format.contains("AIFF") || name.ends_with(".aiff") || name.ends_with(".aif") {
        return Some(QualityTier::Lossless);
    }
    if format.contains("Apple Lossless") || format.contains("ALAC") {
        return Some(QualityTier::Lossless);
    }

    // Ogg Vorbis
    if format.contains("Ogg Vorbis") || name.ends_with(".ogg") {
        return Some(QualityTier::OggVorbis);
    }

    // Opus
    if format.contains("Opus") || name.ends_with(".opus") {
        return Some(QualityTier::Opus);
    }

    // MP3 variants - detect based on format field (codec/bitrate), NOT filename
    if format.contains("MP3") || name.ends_with(".mp3") {
        // Check for low-bitrate from format field (e.g., "64Kbps MP3")
        if let Some(bitrate) = parse_bitrate_from_format(format) {
            if bitrate < 96 {
                return None; // Too low quality, skip
            }
        }

        // VBR detection - from format field only
        if format.contains("VBR") {
            // Try to detect V0 (~245kbps average) vs other VBR
            if let Some(bitrate) = parse_bitrate_from_format(format) {
                if bitrate >= 240 {
                    return Some(QualityTier::Mp3V0); // V0 or equivalent
                }
            }
            // Archive.org often just says "VBR MP3" without bitrate
            return Some(QualityTier::Mp3Vbr);
        }

        // CBR detection from format string (e.g., "192Kbps MP3", "320Kbps MP3")
        if let Some(bitrate) = parse_bitrate_from_format(format) {
            if bitrate >= 320 {
                return Some(QualityTier::Mp3_320);
            } else if bitrate >= 256 {
                return Some(QualityTier::Mp3_256);
            } else if bitrate >= 192 {
                return Some(QualityTier::Mp3_192);
            } else if bitrate >= 128 {
                return Some(QualityTier::Mp3_192); // 128-191 grouped with 192
            } else {
                return None; // Too low quality
            }
        }

        // Default to 192 tier if we can't detect bitrate from format
        return Some(QualityTier::Mp3_192);
    }

    // AAC/M4A
    if format.contains("AAC") || format.contains("M4A") || name.ends_with(".m4a") || name.ends_with(".aac") {
        return Some(QualityTier::Aac);
    }

    None
}

/// Parse bitrate from Archive.org format string (e.g., "192Kbps MP3" -> 192).
fn parse_bitrate_from_format(format: &str) -> Option<u32> {
    // Look for patterns like "192Kbps", "320kbps", "128 kbps"
    let format_lower = format.to_lowercase();
    for word in format_lower.split_whitespace() {
        if let Some(kb_pos) = word.find("kb") {
            if let Ok(bitrate) = word[..kb_pos].parse::<u32>() {
                return Some(bitrate);
            }
        }
    }
    // Also try pattern without space: "192Kbps"
    if let Some(captures) = format_lower.find("kbps").or_else(|| format_lower.find("kb")) {
        let before = &format_lower[..captures];
        if let Some(start) = before.rfind(|c: char| !c.is_ascii_digit()) {
            if let Ok(bitrate) = before[start + 1..].parse::<u32>() {
                return Some(bitrate);
            }
        } else if let Ok(bitrate) = before.parse::<u32>() {
            return Some(bitrate);
        }
    }
    None
}

/// Check if a file should be skipped entirely (not imported in any tier).
fn should_skip_file(file: &ArchiveFileEntry) -> bool {
    let name = file.name.to_lowercase();
    let format = file.format.as_deref().unwrap_or("");

    // Skip archive bundles
    if name.ends_with(".zip") || name.ends_with(".tar") || name.ends_with(".gz") {
        return true;
    }

    // Skip playlist files
    if name.ends_with(".m3u") || name.ends_with(".m3u8") || name.ends_with(".pls") {
        return true;
    }

    // Skip 64kbps variants (too low quality for modern use)
    if name.contains("_64kb") || format.contains("64Kb") {
        return true;
    }

    // Skip sample/preview files
    if name.contains("_sample") || name.contains("_preview") {
        return true;
    }

    false
}

/// Unescape backslash-escaped characters from Archive.org metadata.
/// Archive.org sometimes escapes parentheses: "Cry Over You \(original mix\)"
pub fn unescape_archive_metadata(s: &str) -> String {
    s.replace("\\(", "(")
        .replace("\\)", ")")
        .replace("\\[", "[")
        .replace("\\]", "]")
        .replace("\\'", "'")
        .replace("\\\"", "\"")
}

/// Cover art candidate info for admin review.
#[derive(Debug, Clone, Serialize)]
pub struct CoverCandidate {
    /// Original filename from Archive.org
    pub filename: String,
    /// File size in bytes
    pub size: u64,
    /// Similarity score (0-100, higher = more likely to be cover art)
    pub score: u32,
}

/// Check if a file is an audio file we want to import.
/// Uses quality tier detection which also handles skip logic.
fn is_audio_file(file: &ArchiveFileEntry) -> bool {
    detect_quality_tier(file).is_some()
}

/// Check if a file is a metadata file to include for provenance.
/// Only include _meta.xml - the authoritative Archive.org metadata.
/// Skip _files.xml and _reviews.xml (not needed, can be re-fetched).
fn is_metadata_file(file: &ArchiveFileEntry) -> bool {
    let name = file.name.to_lowercase();

    // Explicitly skip _files.xml and _reviews.xml - these can be re-fetched
    if name.ends_with("_files.xml") || name.ends_with("_reviews.xml") {
        return false;
    }

    // Only include _meta.xml - the core Archive.org item metadata
    if name.ends_with("_meta.xml") {
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

/// Check if a file is an image file (potential cover art).
fn is_image_file(file: &ArchiveFileEntry) -> bool {
    let name = file.name.to_lowercase();

    // Check by format first
    if let Some(format) = &file.format {
        if IMAGE_FORMATS.iter().any(|f| format.contains(f)) {
            return true;
        }
    }

    // Check by extension
    name.ends_with(".jpg")
        || name.ends_with(".jpeg")
        || name.ends_with(".png")
        || name.ends_with(".gif")
}

/// Score a cover art candidate based on filename and size.
/// Higher score = more likely to be the actual cover art.
fn score_cover_candidate(file: &ArchiveFileEntry) -> u32 {
    let name_lower = file.name.to_lowercase();
    let mut score = 0u32;

    // Skip non-image files (meta.txt, etc.)
    if !name_lower.ends_with(".jpg")
        && !name_lower.ends_with(".jpeg")
        && !name_lower.ends_with(".png")
        && !name_lower.ends_with(".gif")
    {
        return 0;
    }

    // Skip derivative/generated files
    // _thumb.jpg = Archive.org thumbnails
    // _spectrogram = audio visualization
    // _meta.txt = metadata files
    if name_lower.contains("_thumb.")
        || name_lower.contains("_spectrogram")
        || name_lower.ends_with("_meta.txt")
    {
        return 0;
    }

    // Penalize size-named versions (prefer full-res originals)
    // e.g., 2016mix_500px.jpg, cover_250px.jpg
    let has_size_suffix = name_lower.contains("_500px")
        || name_lower.contains("_250px")
        || name_lower.contains("_300px")
        || name_lower.contains("_150px")
        || name_lower.contains("_100px")
        || name_lower.contains("_thumb");

    if has_size_suffix {
        // Still include but with low score (might be only option)
        score += 5;
    } else {
        // No size suffix = likely full resolution
        score += 20;
    }

    // Skip itemimage (usually auto-generated derivative)
    if name_lower.contains("_itemimage") {
        return 5; // Very low priority
    }

    // Bonus for known cover art filenames
    for (i, priority_name) in COVER_ART_PRIORITY_NAMES.iter().enumerate() {
        if name_lower == *priority_name || name_lower.ends_with(&format!("/{}", priority_name)) {
            // Higher bonus for earlier entries (more common names)
            score += 50 - (i as u32 * 3);
            break;
        }
    }

    // Bonus for larger files (higher resolution)
    if let Some(size_str) = &file.size {
        if let Ok(size) = size_str.parse::<u64>() {
            // Log scale bonus for size (100KB = 10pts, 1MB = 20pts, 10MB = 30pts)
            let size_kb = size / 1024;
            if size_kb > 0 {
                score += ((size_kb as f64).log10() * 10.0) as u32;
            }
        }
    }

    // Small bonus for jpg/png over gif
    if name_lower.ends_with(".jpg") || name_lower.ends_with(".jpeg") {
        score += 5;
    } else if name_lower.ends_with(".png") {
        score += 3;
    }

    score
}

/// Find cover art candidates from Archive.org files.
/// Returns sorted list (highest score first) with the IA thumbnail info if present.
pub fn find_cover_candidates(files: &[ArchiveFileEntry]) -> (Option<&ArchiveFileEntry>, Vec<CoverCandidate>) {
    let mut ia_thumb: Option<&ArchiveFileEntry> = None;
    let mut candidates: Vec<CoverCandidate> = Vec::new();

    for file in files {
        if !is_image_file(file) {
            continue;
        }

        let name_lower = file.name.to_lowercase();

        // Find the IA thumbnail
        if name_lower == IA_THUMB_FILENAME {
            ia_thumb = Some(file);
            continue; // Don't add thumb to candidates
        }

        let score = score_cover_candidate(file);
        if score > 0 {
            let size = file.size
                .as_ref()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            candidates.push(CoverCandidate {
                filename: file.name.clone(),
                size,
                score,
            });
        }
    }

    // Sort by score descending
    candidates.sort_by(|a, b| b.score.cmp(&a.score));

    (ia_thumb, candidates)
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
    } else if name.ends_with(".xml") {
        Some("application/xml".to_string())
    } else if name.ends_with(".json") {
        Some("application/json".to_string())
    } else {
        None
    }
}

/// Download and upload a single file to Archivist.
/// Returns (cid, size) on success, None only if file is 404 (not found).
///
/// Uses rate limiter for Archive.org requests and retries FOREVER with
/// exponential backoff for transient errors. Only gives up on 404.
async fn upload_file(
    client: &Client,
    archivist_url: &str,
    identifier: &str,
    file_name: &str,
    dest_name: &str,
    auth: Option<&AuthCredentials>,
    buffer_pool: &std::sync::Arc<BufferPool>,
    rate_limiter: &ArchiveRateLimiter,
) -> Option<(String, u64)> {
    let download_url = format!(
        "https://archive.org/download/{}/{}",
        identifier,
        urlencoding::encode(file_name)
    );

    // Acquire buffer slot FIRST - this is what actually limits concurrency
    // The slot is held for the entire download+upload cycle
    let mut slot = buffer_pool.acquire().await;

    // Retry loop - continues forever until success or 404
    let mut attempt: u32 = 0;
    loop {
        if attempt > 0 {
            // Clear buffer for retry
            slot.clear();

            // Exponential backoff with cap at MAX_RETRY_DELAY
            let delay_ms = std::cmp::min(
                RETRY_BASE_DELAY_MS * (1u64 << std::cmp::min(attempt - 1, 10)),
                MAX_RETRY_DELAY_MS,
            );
            let delay = Duration::from_millis(delay_ms);

            if attempt % 5 == 0 {
                // Log every 5th retry at warn level
                warn!(
                    file = %file_name,
                    attempt = attempt,
                    delay_secs = delay.as_secs(),
                    "Still retrying download..."
                );
            } else {
                debug!(
                    file = %file_name,
                    attempt = attempt,
                    delay_ms = delay_ms,
                    "Retrying download"
                );
            }
            tokio::time::sleep(delay).await;
        }
        attempt += 1;

        // Wait for adaptive rate limiter before making Archive.org request
        rate_limiter.wait().await;

        debug!(file = %file_name, dest = %dest_name, attempt = attempt, "Downloading from Archive.org");

        let response = match client.get(&download_url).send().await {
            Ok(r) => r,
            Err(e) => {
                rate_limiter.report_transient_error();
                warn!(file = %file_name, error = %e, attempt = attempt, "Download connection error, will retry");
                continue;
            }
        };

        let status = response.status();

        // 404 = file doesn't exist, give up
        if status.as_u16() == 404 {
            warn!(file = %file_name, "File not found (404), skipping");
            return None;
        }

        // 429, 503, 401, 403 = likely rate limited, back off aggressively
        // Archive.org sometimes returns 401/403 when rate limiting
        if matches!(status.as_u16(), 429 | 503 | 401 | 403) {
            rate_limiter.report_rate_limit_error();
            warn!(
                file = %file_name,
                status = %status,
                url = %download_url,
                attempt = attempt,
                "Rate limited (or auth error), backing off"
            );
            continue;
        }

        // Other client errors (4xx except 404, 429) = permanent failure
        if status.is_client_error() {
            // Get response body for debugging (may contain error details)
            let error_body = response.text().await.unwrap_or_default();
            let body_preview = if error_body.len() > 200 {
                format!("{}...", &error_body[..200])
            } else {
                error_body
            };
            warn!(
                file = %file_name,
                status = %status,
                url = %download_url,
                body = %body_preview,
                "Client error (possibly rate limited), skipping"
            );
            return None;
        }

        // Server errors (5xx except 503) = retry with transient backoff
        if !status.is_success() {
            rate_limiter.report_transient_error();
            warn!(file = %file_name, status = %status, attempt = attempt, "Server error, will retry");
            continue;
        }

        // Get content-type from response, but override for known file types
        let content_type = mime_from_name(file_name)
            .or_else(|| {
                response
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string())
            });

        // Clear buffer before streaming (in case of retry within same slot)
        slot.clear();

        // Stream download into buffer
        let download_start = std::time::Instant::now();
        let mut stream = response.bytes_stream();
        let mut stream_error = false;

        while let Some(chunk_result) = stream.next().await {
            let chunk = match chunk_result {
                Ok(c) => c,
                Err(e) => {
                    warn!(file = %file_name, error = %e, attempt = attempt, "Stream error, will retry");
                    stream_error = true;
                    break;
                }
            };
            if let Err(e) = slot.write_chunk(&chunk) {
                warn!(file = %file_name, error = %e, "Buffer write error, skipping");
                return None;
            }
        }

        if stream_error {
            continue; // Retry the whole download
        }

        let size = slot.size();
        let download_secs = download_start.elapsed().as_secs_f64();
        let speed_mbps = if download_secs > 0.0 { (size as f64 / 1024.0 / 1024.0) / download_secs } else { 0.0 };

        // Report success to adaptive rate limiter (may speed up)
        rate_limiter.report_success();

        info!(
            file = %file_name,
            size_mb = size / 1024 / 1024,
            speed_mbps = format!("{:.1}", speed_mbps),
            storage = slot.storage_type(),
            "Download complete"
        );

        // Upload to Archivist (also retries forever)
        let bytes = match slot.into_bytes() {
            Ok(b) => b,
            Err(e) => {
                warn!(file = %file_name, error = %e, "Failed to read buffer, skipping");
                return None;
            }
        };

        let upload_url = format!("{}/api/archivist/v1/data", archivist_url);

        // Retry upload forever
        let mut upload_attempt: u32 = 0;
        loop {
            if upload_attempt > 0 {
                let delay_ms = std::cmp::min(
                    RETRY_BASE_DELAY_MS * (1u64 << std::cmp::min(upload_attempt - 1, 10)),
                    MAX_RETRY_DELAY_MS,
                );
                let delay = Duration::from_millis(delay_ms);
                if upload_attempt % 5 == 0 {
                    warn!(file = %file_name, attempt = upload_attempt, "Still retrying upload...");
                }
                tokio::time::sleep(delay).await;
            }
            upload_attempt += 1;

            let mut request = client
                .post(&upload_url)
                .header("Content-Disposition", format!("attachment; filename=\"{}\"", dest_name))
                .body(bytes.clone());

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
                    warn!(file = %file_name, error = %e, attempt = upload_attempt, "Upload error, will retry");
                    continue;
                }
            };

            let upload_status = upload_response.status();

            if upload_status.is_server_error() {
                warn!(file = %file_name, status = %upload_status, attempt = upload_attempt, "Upload server error, will retry");
                continue;
            }

            if !upload_status.is_success() {
                let error_body = upload_response.text().await.unwrap_or_default();
                warn!(
                    file = %file_name,
                    status = %upload_status,
                    error = %error_body,
                    "Upload failed permanently, skipping"
                );
                return None;
            }

            let cid = match upload_response.text().await {
                Ok(text) => text.trim().to_string(),
                Err(e) => {
                    warn!(file = %file_name, error = %e, attempt = upload_attempt, "Failed to read CID, will retry");
                    continue;
                }
            };

            info!(file = %file_name, cid = %cid, "Upload complete");
            return Some((cid, size));
        }
    }
}


/// Execute an import from Archive.org to Archivist.
///
/// # Arguments
/// * `client` - HTTP client
/// * `archivist_url` - Base URL of Archivist API
/// * `identifier` - Archive.org item identifier
/// * `auth` - Optional authentication credentials for Archivist uploads
/// * `buffer_pool` - Shared buffer pool for downloads
/// * `rate_limiter` - Global rate limiter for Archive.org API requests
/// * `on_progress` - Callback for progress updates (0.0-1.0, optional message)
pub async fn execute_import<F>(
    client: &Client,
    archivist_url: &str,
    identifier: &str,
    auth: Option<&AuthCredentials>,
    buffer_pool: &std::sync::Arc<BufferPool>,
    rate_limiter: &ArchiveRateLimiter,
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

    let total_files = audio_files.len() + metadata_files.len();
    on_progress(0.05, Some(format!("Found {} files, downloading in parallel", total_files)));

    // 4. Group audio files by quality tier, DEDUPLICATING by track identity
    // Track identity = track number (from tags) or title. NOT filename.
    // When multiple files have the same track in the same tier, prefer derivatives.
    let mut tier_files: HashMap<QualityTier, HashMap<String, &ArchiveFileEntry>> = HashMap::new();

    for file in &audio_files {
        if let Some(tier) = detect_quality_tier(file) {
            // Track identity: use track number if available, else title, else filename stem
            let track_id = file.track.clone()
                .or_else(|| file.title.clone())
                .unwrap_or_else(|| {
                    // Fallback: extract base name without extension and suffixes
                    let stem = file.name.rsplit('/').next().unwrap_or(&file.name);
                    let stem = stem.split('.').next().unwrap_or(stem);
                    // Remove common suffixes like _vbr, _64kb
                    stem.trim_end_matches("_vbr")
                        .trim_end_matches("_64kb")
                        .trim_end_matches("_128kb")
                        .trim_end_matches("_192kb")
                        .to_string()
                });

            let tier_map = tier_files.entry(tier).or_default();

            // If we already have this track in this tier, prefer derivative (smaller/optimized)
            if let Some(existing) = tier_map.get(&track_id) {
                let existing_is_derivative = existing.source.as_deref() == Some("derivative");
                let new_is_derivative = file.source.as_deref() == Some("derivative");

                // Prefer derivative over original
                if new_is_derivative && !existing_is_derivative {
                    debug!(
                        tier = %tier.as_str(),
                        track = %track_id,
                        old_file = %existing.name,
                        new_file = %file.name,
                        "Replacing original with derivative"
                    );
                    tier_map.insert(track_id, file);
                }
                // Otherwise keep existing (first derivative wins, or first original if no derivatives)
            } else {
                tier_map.insert(track_id, file);
            }
        }
    }

    // Log what we found
    for (tier, tracks) in &tier_files {
        info!(
            identifier = %identifier,
            tier = %tier.as_str(),
            track_count = tracks.len(),
            "Quality tier"
        );
    }

    // 5. Download and upload files IN PARALLEL
    // The buffer pool's semaphore limits concurrent downloads globally
    // So with --concurrent=8, we download up to 8 files at once across all jobs

    // Prepare audio file tasks from deduplicated tier map
    let audio_tasks: Vec<_> = tier_files.values()
        .flat_map(|tracks| tracks.values())
        .filter_map(|file| {
            let tier = detect_quality_tier(file)?;
            let track_num = parse_track_number(&file.track);
            let nice_name = generate_track_filename(file, track_num);
            let original_name = file.name.clone();
            // Unescape Archive.org metadata (e.g., "\(" → "(")
            let title = file.title.as_ref().map(|t| unescape_archive_metadata(t));
            let artist = file.artist.as_ref().map(|a| unescape_archive_metadata(a));
            let duration = parse_duration(&file.length);

            Some((original_name, nice_name, track_num, title, artist, duration, false, Some(tier)))
        }).collect();

    // Prepare metadata file tasks (no tier for metadata)
    let meta_tasks: Vec<_> = metadata_files.iter().map(|file| {
        let dest_name = if file.name.starts_with('_') {
            file.name.clone()
        } else {
            format!("_{}", file.name)
        };
        let original_name = file.name.clone();

        (original_name, dest_name, None, None, None, None, true, None::<QualityTier>)
    }).collect();

    // Combine all tasks
    let all_tasks: Vec<_> = audio_tasks.into_iter().chain(meta_tasks).collect();

    // Process all files in parallel using buffer_unordered
    // The buffer pool's acquire() naturally throttles to max_concurrent
    // The rate limiter ensures we're good citizens to Archive.org
    let results: Vec<_> = stream::iter(all_tasks)
        .map(|(original_name, dest_name, track_num, title, artist, duration, is_meta, tier)| {
            let client = client.clone();
            let archivist_url = archivist_url.to_string();
            let identifier = identifier.to_string();
            let auth = auth.cloned();
            let buffer_pool = Arc::clone(buffer_pool);
            let rate_limiter = Arc::clone(rate_limiter);

            async move {
                let result = upload_file(
                    &client,
                    &archivist_url,
                    &identifier,
                    &original_name,
                    &dest_name,
                    auth.as_ref(),
                    &buffer_pool,
                    &rate_limiter,
                ).await;

                (result, dest_name, original_name, track_num, title, artist, duration, is_meta, tier)
            }
        })
        .buffer_unordered(buffer_pool.max_concurrent()) // Process up to max_concurrent at once
        .collect()
        .await;

    // Determine the primary (best) tier for track metadata
    // We only want track metadata from ONE tier, not duplicated across all tiers
    let primary_tier = tier_files.keys()
        .max_by_key(|t| t.priority())
        .copied();

    debug!(
        primary_tier = ?primary_tier.map(|t| t.as_str()),
        all_tiers = ?tier_files.keys().map(|t| t.as_str()).collect::<Vec<_>>(),
        "Selected primary tier for track metadata"
    );

    // Collect results - organize by tier for separate directory CIDs
    let mut uploaded_entries: Vec<DirectoryEntry> = Vec::new();
    let mut track_metadata: Vec<TrackMetadata> = Vec::new();
    let mut tier_entries: HashMap<QualityTier, Vec<DirectoryEntry>> = HashMap::new();
    let mut metadata_entries: Vec<DirectoryEntry> = Vec::new();

    for (result, dest_name, original_name, track_num, title, artist, duration, is_meta, tier) in results {
        if let Some((cid, size)) = result {
            let mimetype = if is_meta {
                if original_name.ends_with(".xml") {
                    Some("application/xml".to_string())
                } else if original_name.ends_with(".json") {
                    Some("application/json".to_string())
                } else {
                    Some("text/plain".to_string())
                }
            } else {
                mime_from_name(&original_name)
            };

            let entry = DirectoryEntry {
                path: dest_name.clone(),
                cid,
                size,
                mimetype,
            };

            if is_meta {
                metadata_entries.push(entry.clone());
            } else if let Some(t) = tier {
                tier_entries.entry(t).or_default().push(entry.clone());
            }

            uploaded_entries.push(entry);

            // Only add track metadata from the PRIMARY tier to avoid duplicates
            // e.g., if we have Mp3_192, Mp3Vbr, and OggVorbis tiers, only use
            // the highest-priority tier's tracks for the track listing
            if !is_meta && tier == primary_tier {
                track_metadata.push(TrackMetadata {
                    track_number: track_num,
                    title,
                    artist,
                    duration,
                    filename: dest_name,
                    original_filename: original_name,
                });
            }
        }
    }

    on_progress(0.90, Some(format!("Uploaded {} files", uploaded_entries.len())));

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

    // 6. Create separate directory CIDs for each quality tier
    on_progress(0.94, Some("Creating quality tier manifests".to_string()));

    let mut quality_tiers_map: HashMap<String, String> = HashMap::new();

    for (tier, mut entries) in tier_entries {
        // Sort entries by path (track order)
        entries.sort_by(|a, b| a.path.cmp(&b.path));

        // Include metadata files in each tier directory for provenance
        let mut tier_entries_with_meta = entries.clone();
        tier_entries_with_meta.extend(metadata_entries.clone());

        let tier_request = serde_json::json!({
            "entries": tier_entries_with_meta,
        });

        let mut tier_req = client.post(&directory_url).json(&tier_request);
        if let Some(creds) = auth {
            tier_req = tier_req
                .header("X-Pubkey", &creds.pubkey)
                .header("X-Timestamp", creds.timestamp.to_string())
                .header("X-Signature", &creds.signature);
        }

        match tier_req.send().await {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(tier_dir) = resp.json::<DirectoryResponse>().await {
                    info!(
                        tier = %tier.as_str(),
                        cid = %tier_dir.cid,
                        files = entries.len(),
                        "Created tier directory"
                    );
                    quality_tiers_map.insert(tier.as_str().to_string(), tier_dir.cid);
                }
            }
            Ok(resp) => {
                warn!(
                    tier = %tier.as_str(),
                    status = %resp.status(),
                    "Failed to create tier directory"
                );
            }
            Err(e) => {
                warn!(
                    tier = %tier.as_str(),
                    error = %e,
                    "Failed to create tier directory"
                );
            }
        }
    }

    info!(
        identifier = %identifier,
        tiers = ?quality_tiers_map.keys().collect::<Vec<_>>(),
        "Created {} quality tier directories",
        quality_tiers_map.len()
    );

    // Precedence: Archive.org item metadata > ID3 tags
    //
    // Archive.org's item-level metadata is curated and authoritative.
    // ID3 tags from individual tracks are often inconsistent or wrong
    // (e.g., one track might have "Remix by X" as album name).
    //
    // For title: Archive.org title > ID3 album tag
    // For artist: Archive.org creator > ID3 artist tag
    let final_title = album_title.clone()
        .or_else(|| audio_files.iter().find_map(|f| f.album.clone()));

    let final_artist = album_artist.clone()
        .or_else(|| audio_files.iter().find_map(|f| f.artist.clone()));

    // Serialize license info to JSON for Flagship compatibility
    let license_json = license_info
        .as_ref()
        .and_then(|li| serde_json::to_string(li).ok());

    // Sort track metadata by track number and serialize to Flagship format
    track_metadata.sort_by_key(|t| t.track_number.unwrap_or(999));

    let track_metadata_json = serialize_track_metadata(&track_metadata, final_artist.as_deref());

    // Find cover art candidates
    let (_, cover_candidates) = find_cover_candidates(&metadata.files);

    // Determine primary audio quality from the best available tier
    let primary_audio_quality = tier_files.keys()
        .max_by_key(|t| t.priority())
        .map(|t| t.to_audio_quality());

    info!(
        identifier = %identifier,
        cid = %directory.cid,
        audio_files = audio_count,
        title = ?final_title,
        artist = ?final_artist,
        license = ?license_info,
        cover_candidates = cover_candidates.len(),
        audio_quality = ?primary_audio_quality,
        "Import complete"
    );

    on_progress(1.0, Some("Import complete".to_string()));

    Ok(JobResult::Import {
        files_imported: audio_count,
        directory_cid: directory.cid,
        quality_tiers: quality_tiers_map,
        source: format!("archive.org:{}", identifier),
        title: final_title,
        artist: final_artist,
        date: album_date,
        license: license_json,
        track_metadata: track_metadata_json,
        thumbnail_cid: None, // Set by cover art processing in mod.rs
        audio_quality: primary_audio_quality,
    })
}

/// Serialize track metadata to Flagship's JSON format.
/// Format: [{"title": "...", "artist": "...", "duration": "3:45", "trackNumber": 1}, ...]
fn serialize_track_metadata(tracks: &[TrackMetadata], album_artist: Option<&str>) -> Option<String> {
    if tracks.is_empty() {
        return None;
    }

    let flagship_tracks: Vec<serde_json::Value> = tracks
        .iter()
        .map(|t| {
            let mut obj = serde_json::Map::new();

            if let Some(title) = &t.title {
                obj.insert("title".to_string(), serde_json::Value::String(title.clone()));
            }

            // Use track artist if different from album, otherwise use album artist
            let artist = t.artist.as_deref().or(album_artist);
            if let Some(a) = artist {
                obj.insert("artist".to_string(), serde_json::Value::String(a.to_string()));
            }

            // Format duration as "M:SS" like Flagship expects
            if let Some(secs) = t.duration {
                let mins = (secs / 60.0) as u32;
                let remaining_secs = (secs % 60.0) as u32;
                obj.insert(
                    "duration".to_string(),
                    serde_json::Value::String(format!("{}:{:02}", mins, remaining_secs)),
                );
            }

            if let Some(num) = t.track_number {
                obj.insert(
                    "trackNumber".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(num)),
                );
            }

            serde_json::Value::Object(obj)
        })
        .collect();

    serde_json::to_string(&flagship_tracks).ok()
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
        assert!(!is_metadata_file(&make_file("item_files.xml", Some("Text")))); // Skip _files.xml
        assert!(!is_metadata_file(&make_file("item_reviews.xml", Some("Text")))); // Skip _reviews.xml
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

    #[test]
    fn test_unescape_archive_metadata() {
        // Real-world case: Archive.org escapes parentheses
        assert_eq!(
            unescape_archive_metadata(r"Cry Over You \(original mix\)"),
            "Cry Over You (original mix)"
        );
        // Multiple escapes
        assert_eq!(
            unescape_archive_metadata(r"Song \[Remix\] \(Extended\)"),
            "Song [Remix] (Extended)"
        );
        // Quotes (use regular string with double escaping for Rust)
        assert_eq!(
            unescape_archive_metadata("He said \\\"hello\\\""),
            "He said \"hello\""
        );
        // Already clean - no change
        assert_eq!(
            unescape_archive_metadata("Normal Title (No Escapes)"),
            "Normal Title (No Escapes)"
        );
    }

    /// Helper to create a file with full metadata for deduplication tests
    fn make_file_full(
        name: &str,
        format: Option<&str>,
        source: Option<&str>,
        track: Option<&str>,
        title: Option<&str>,
    ) -> ArchiveFileEntry {
        ArchiveFileEntry {
            name: name.to_string(),
            format: format.map(String::from),
            size: Some("1000000".to_string()),
            source: source.map(String::from),
            title: title.map(String::from),
            artist: None,
            album: None,
            track: track.map(String::from),
            length: None,
            genre: None,
        }
    }

    /// Test case for tou247 - Archive.org item with duplicate files per track
    /// https://archive.org/details/tou247
    ///
    /// This item has 2 tracks ("The Piano Tune" and "Too Retro") but Archive.org
    /// provides both original and derivative versions:
    /// - tou247a.mp3 (original, "VBR MP3") - "The Piano Tune"
    /// - tou247a_vbr.mp3 (derivative, "VBR MP3") - "The Piano Tune"
    /// - tou247b.mp3 (original, "VBR MP3") - "Too Retro"
    /// - tou247b_vbr.mp3 (derivative, "VBR MP3") - "Too Retro"
    ///
    /// We should deduplicate to 2 tracks per tier, preferring derivatives.
    #[test]
    fn test_tou247_track_deduplication() {
        // Simulate the Archive.org file entries for tou247
        let files = vec![
            make_file_full("tou247a.mp3", Some("VBR MP3"), Some("original"), Some("1"), Some("The Piano Tune")),
            make_file_full("tou247a_vbr.mp3", Some("VBR MP3"), Some("derivative"), Some("1"), Some("The Piano Tune")),
            make_file_full("tou247b.mp3", Some("VBR MP3"), Some("original"), Some("2"), Some("Too Retro")),
            make_file_full("tou247b_vbr.mp3", Some("VBR MP3"), Some("derivative"), Some("2"), Some("Too Retro")),
        ];

        // Run the deduplication logic
        let mut tier_files: HashMap<QualityTier, HashMap<String, &ArchiveFileEntry>> = HashMap::new();

        for file in &files {
            if let Some(tier) = detect_quality_tier(file) {
                // Track identity: use track number if available, else title, else filename stem
                let track_id = file.track.clone()
                    .or_else(|| file.title.clone())
                    .unwrap_or_else(|| {
                        let stem = file.name.rsplit('/').next().unwrap_or(&file.name);
                        let stem = stem.split('.').next().unwrap_or(stem);
                        stem.trim_end_matches("_vbr")
                            .trim_end_matches("_64kb")
                            .trim_end_matches("_128kb")
                            .trim_end_matches("_192kb")
                            .to_string()
                    });

                let tier_map = tier_files.entry(tier).or_default();

                if let Some(existing) = tier_map.get(&track_id) {
                    let existing_is_derivative = existing.source.as_deref() == Some("derivative");
                    let new_is_derivative = file.source.as_deref() == Some("derivative");

                    // Prefer derivative over original
                    if new_is_derivative && !existing_is_derivative {
                        tier_map.insert(track_id, file);
                    }
                } else {
                    tier_map.insert(track_id, file);
                }
            }
        }

        // All files should be Mp3Vbr tier
        assert_eq!(tier_files.len(), 1, "Should have exactly 1 quality tier");
        assert!(tier_files.contains_key(&QualityTier::Mp3Vbr), "Should be Mp3Vbr tier");

        // Should have 2 tracks (not 4!)
        let vbr_tracks = tier_files.get(&QualityTier::Mp3Vbr).unwrap();
        assert_eq!(vbr_tracks.len(), 2, "Should have exactly 2 tracks after deduplication");

        // Both should be derivatives
        for (track_id, file) in vbr_tracks {
            assert_eq!(
                file.source.as_deref(),
                Some("derivative"),
                "Track {} should be derivative, got {:?} (file: {})",
                track_id, file.source, file.name
            );
        }

        // Verify track identities
        let track_ids: std::collections::HashSet<_> = vbr_tracks.keys().collect();
        assert!(track_ids.contains(&"1".to_string()), "Should have track 1");
        assert!(track_ids.contains(&"2".to_string()), "Should have track 2");
    }

    /// Test that tracks without tags still deduplicate by filename stem
    #[test]
    fn test_deduplication_fallback_to_filename() {
        // Files without track tags - should deduplicate by filename stem
        let files = vec![
            make_file_full("track01.mp3", Some("VBR MP3"), Some("original"), None, None),
            make_file_full("track01_vbr.mp3", Some("VBR MP3"), Some("derivative"), None, None),
            make_file_full("track02.mp3", Some("VBR MP3"), Some("original"), None, None),
            make_file_full("track02_vbr.mp3", Some("VBR MP3"), Some("derivative"), None, None),
        ];

        let mut tier_files: HashMap<QualityTier, HashMap<String, &ArchiveFileEntry>> = HashMap::new();

        for file in &files {
            if let Some(tier) = detect_quality_tier(file) {
                let track_id = file.track.clone()
                    .or_else(|| file.title.clone())
                    .unwrap_or_else(|| {
                        let stem = file.name.rsplit('/').next().unwrap_or(&file.name);
                        let stem = stem.split('.').next().unwrap_or(stem);
                        stem.trim_end_matches("_vbr")
                            .trim_end_matches("_64kb")
                            .trim_end_matches("_128kb")
                            .trim_end_matches("_192kb")
                            .to_string()
                    });

                let tier_map = tier_files.entry(tier).or_default();

                if let Some(existing) = tier_map.get(&track_id) {
                    let existing_is_derivative = existing.source.as_deref() == Some("derivative");
                    let new_is_derivative = file.source.as_deref() == Some("derivative");

                    if new_is_derivative && !existing_is_derivative {
                        tier_map.insert(track_id, file);
                    }
                } else {
                    tier_map.insert(track_id, file);
                }
            }
        }

        let vbr_tracks = tier_files.get(&QualityTier::Mp3Vbr).unwrap();
        assert_eq!(vbr_tracks.len(), 2, "Should have 2 tracks after deduplication");

        // Both should be derivatives (with _vbr suffix removed for matching)
        for (_track_id, file) in vbr_tracks {
            assert_eq!(file.source.as_deref(), Some("derivative"));
        }
    }

    /// Test quality tier detection uses format field, not filename
    #[test]
    fn test_quality_tier_uses_format_not_filename() {
        // File with _vbr in name but no VBR in format should NOT be classified as VBR
        let file_cbr = make_file_full("track_vbr.mp3", Some("192Kbps MP3"), None, None, None);
        assert_eq!(detect_quality_tier(&file_cbr), Some(QualityTier::Mp3_192));

        // File without _vbr in name but WITH VBR in format SHOULD be classified as VBR
        let file_vbr = make_file_full("track.mp3", Some("VBR MP3"), None, None, None);
        assert_eq!(detect_quality_tier(&file_vbr), Some(QualityTier::Mp3Vbr));
    }
}
