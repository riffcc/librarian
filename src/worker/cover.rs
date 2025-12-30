//! Cover art detection and processing.
//!
//! Supports multiple cover art sources:
//! 1. Embedded cover art from audio files (ID3 APIC, FLAC PICTURE)
//! 2. Image files in directories (cover.jpg, folder.png, etc.)
//! 3. Archive.org thumbnail comparison using perceptual hashing
//! 4. Explicitly provided cover CIDs
//!
//! Processing:
//! - Resize to max 1024x1024 (no upscaling)
//! - Generate WebP (85% quality) as main format
//! - Generate JPG fallback for compatibility
//! - Keep original high-res if available

use image::{DynamicImage, GenericImageView, imageops::FilterType};
use image_hasher::{HasherConfig, ImageHash};
use reqwest::Client;
use serde::{Serialize, Deserialize};
use tracing::{debug, info, warn};

use crate::auth::AuthCredentials;

/// Minimum similarity score (0-100) to consider a match.
/// Lower Hamming distance = higher similarity.
/// Hash size is 64 bits, so max distance is 64.
const MIN_SIMILARITY_THRESHOLD: u32 = 70;

/// Maximum number of candidate images to download and compare.
const MAX_CANDIDATES_TO_CHECK: usize = 5;

/// Maximum dimension for main cover art (1024x1024).
const MAX_COVER_DIMENSION: u32 = 1024;

/// WebP quality for cover art (0-100).
const WEBP_QUALITY: f32 = 85.0;

/// JPG quality for fallback (0-100).
const JPG_QUALITY: u8 = 90;

/// PNG compression level (0-9, higher = smaller file but slower).
const PNG_COMPRESSION: image::codecs::png::CompressionType = image::codecs::png::CompressionType::Best;

/// Threshold for "few colors" detection - if unique colors are below this,
/// PNG is likely better than JPG.
const FEW_COLORS_THRESHOLD: usize = 256;

// =============================================================================
// Enhanced Cover Art Types
// =============================================================================

/// Source of cover art.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CoverSource {
    /// Embedded in audio file (ID3 APIC, FLAC PICTURE)
    Embedded,
    /// Image file in directory
    DirectoryFile,
    /// From Archive.org
    ArchiveOrg,
    /// Explicitly provided CID
    ProvidedCid,
}

/// Fallback format for cover art (when WebP isn't supported).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FallbackFormat {
    /// JPEG - best for photographic images
    Jpg,
    /// PNG - best for images with transparency or few colors (logos, vector art)
    Png,
}

/// Processed cover art ready for upload.
#[derive(Debug)]
pub struct ProcessedCover {
    /// WebP version (main format)
    pub webp_data: Vec<u8>,
    /// Fallback version (JPG or PNG depending on image characteristics)
    pub fallback_data: Vec<u8>,
    /// Format of the fallback version
    pub fallback_format: FallbackFormat,
    /// Original image dimensions (before resize)
    pub original_width: u32,
    pub original_height: u32,
    /// Final dimensions (after resize)
    pub final_width: u32,
    pub final_height: u32,
    /// Whether the image was resized
    pub was_resized: bool,
    /// Source of the cover art
    pub source: CoverSource,
}

/// Result of uploading processed cover art.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoverUploadResult {
    /// Main cover CID (WebP, max 1024x1024)
    pub main_cid: String,
    /// Fallback cover CID (JPG or PNG)
    pub fallback_cid: String,
    /// Format of the fallback version
    pub fallback_format: FallbackFormat,
    /// High-res original CID (if source was larger than 1024x1024)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highres_cid: Option<String>,
    /// Original dimensions
    pub original_width: u32,
    pub original_height: u32,
    /// Source of the cover art
    pub source: CoverSource,
}

/// Cover art candidate with similarity info.
#[derive(Debug, Clone, Serialize)]
pub struct CoverMatch {
    /// Original filename from Archive.org
    pub filename: String,
    /// Similarity score (0-100, higher = more similar)
    pub similarity: u32,
    /// File size in bytes
    pub size: u64,
    /// Archivist CID after upload (WebP version)
    pub cid: Option<String>,
}

/// Result of cover art detection.
#[derive(Debug)]
pub struct CoverArtResult {
    /// Best match CID (WebP, uploaded to Archivist)
    pub thumbnail_cid: Option<String>,
    /// All candidates found (for admin review if no confident match)
    pub candidates: Vec<CoverMatch>,
    /// Whether we found a confident match
    pub confident: bool,
}

/// Download an image from Archive.org and decode it.
async fn download_image(client: &Client, identifier: &str, filename: &str) -> Option<DynamicImage> {
    let url = format!(
        "https://archive.org/download/{}/{}",
        identifier,
        urlencoding::encode(filename)
    );

    let response = client.get(&url).send().await.ok()?;
    if !response.status().is_success() {
        return None;
    }

    let bytes = response.bytes().await.ok()?;
    image::load_from_memory(&bytes).ok()
}

/// Compute perceptual hash of an image.
fn compute_hash(image: &DynamicImage) -> ImageHash {
    let hasher = HasherConfig::new()
        .hash_size(8, 8) // 64-bit hash
        .to_hasher();
    hasher.hash_image(image)
}

/// Calculate similarity percentage from Hamming distance.
/// Hash is 64 bits, so max distance is 64.
fn hamming_to_similarity(distance: u32) -> u32 {
    let max_distance = 64u32;
    ((max_distance.saturating_sub(distance)) * 100) / max_distance
}

/// Encode image to WebP at configured quality.
/// Returns None if encoding fails.
fn encode_webp(image: &DynamicImage) -> Option<Vec<u8>> {
    let encoder = webp::Encoder::from_image(image).ok()?;
    let webp_data = encoder.encode(WEBP_QUALITY);
    Some(webp_data.to_vec())
}

/// Encode image to JPG at configured quality.
/// Returns None if encoding fails.
fn encode_jpg(image: &DynamicImage) -> Option<Vec<u8>> {
    use std::io::Cursor;
    use image::codecs::jpeg::JpegEncoder;

    let mut buffer = Cursor::new(Vec::new());
    let rgb = image.to_rgb8();
    let mut encoder = JpegEncoder::new_with_quality(&mut buffer, JPG_QUALITY);
    encoder.encode_image(&rgb).ok()?;
    Some(buffer.into_inner())
}

/// Encode image to PNG with optimal compression.
/// Returns None if encoding fails.
fn encode_png(image: &DynamicImage) -> Option<Vec<u8>> {
    use std::io::Cursor;
    use image::codecs::png::PngEncoder;

    let mut buffer = Cursor::new(Vec::new());
    let encoder = PngEncoder::new_with_quality(
        &mut buffer,
        PNG_COMPRESSION,
        image::codecs::png::FilterType::Adaptive,
    );
    image.write_with_encoder(encoder).ok()?;
    Some(buffer.into_inner())
}

/// Determine if PNG is a better fallback format than JPG.
///
/// PNG is better for:
/// - Images with transparency (alpha channel)
/// - Images with few unique colors (logos, vector art, simple graphics)
/// - Images with sharp edges that would blur with JPG compression
fn should_use_png(image: &DynamicImage) -> bool {
    // Check for transparency
    if image.color().has_alpha() {
        // Sample some pixels to check if transparency is actually used
        let rgba = image.to_rgba8();
        let (width, height) = image.dimensions();
        let sample_count = 100.min((width * height) as usize);
        let step = (width * height) as usize / sample_count;

        for (i, pixel) in rgba.pixels().enumerate() {
            if i % step == 0 && pixel[3] < 255 {
                debug!("Image has transparency, using PNG fallback");
                return true;
            }
        }
    }

    // Check for few unique colors (suggests logo/vector art)
    // Sample a subset of pixels for performance
    let rgb = image.to_rgb8();
    let (width, height) = image.dimensions();
    let total_pixels = (width * height) as usize;

    // For large images, sample; for small images, check all
    let sample_step = if total_pixels > 10000 { total_pixels / 5000 } else { 1 };

    let mut unique_colors: std::collections::HashSet<[u8; 3]> = std::collections::HashSet::new();
    for (i, pixel) in rgb.pixels().enumerate() {
        if i % sample_step == 0 {
            unique_colors.insert([pixel[0], pixel[1], pixel[2]]);
            // Early exit if we've found enough colors to know it's photographic
            if unique_colors.len() > FEW_COLORS_THRESHOLD {
                break;
            }
        }
    }

    if unique_colors.len() <= FEW_COLORS_THRESHOLD {
        debug!(
            color_count = unique_colors.len(),
            "Image has few colors, using PNG fallback"
        );
        return true;
    }

    false
}

/// Resize image to fit within max dimension while preserving aspect ratio.
/// Does NOT upscale - if image is smaller than max, returns original.
///
/// Returns (resized_image, was_resized).
pub fn resize_to_max(image: &DynamicImage, max_dimension: u32) -> (DynamicImage, bool) {
    let (width, height) = image.dimensions();

    // Don't upscale
    if width <= max_dimension && height <= max_dimension {
        return (image.clone(), false);
    }

    // Calculate new dimensions preserving aspect ratio
    let (new_width, new_height) = if width > height {
        let ratio = max_dimension as f64 / width as f64;
        (max_dimension, (height as f64 * ratio) as u32)
    } else {
        let ratio = max_dimension as f64 / height as f64;
        ((width as f64 * ratio) as u32, max_dimension)
    };

    let resized = image.resize(new_width, new_height, FilterType::Lanczos3);
    (resized, true)
}

/// Process cover art image: resize and encode to multiple formats.
///
/// - Resizes to max 1024x1024 (no upscaling)
/// - Encodes to WebP (main format)
/// - Encodes to PNG or JPG fallback based on image characteristics
///   (PNG for transparency or few colors, JPG for photographic images)
pub fn process_cover_image(
    image: &DynamicImage,
    source: CoverSource,
) -> Option<ProcessedCover> {
    let (original_width, original_height) = image.dimensions();

    // Resize if needed
    let (resized, was_resized) = resize_to_max(image, MAX_COVER_DIMENSION);
    let (final_width, final_height) = resized.dimensions();

    // Encode to WebP (main format)
    let webp_data = encode_webp(&resized)?;

    // Choose fallback format based on image characteristics
    let (fallback_data, fallback_format) = if should_use_png(&resized) {
        (encode_png(&resized)?, FallbackFormat::Png)
    } else {
        (encode_jpg(&resized)?, FallbackFormat::Jpg)
    };

    debug!(
        original = format!("{}x{}", original_width, original_height),
        final_size = format!("{}x{}", final_width, final_height),
        was_resized = was_resized,
        webp_size = webp_data.len(),
        fallback_size = fallback_data.len(),
        fallback_format = ?fallback_format,
        "Processed cover art"
    );

    Some(ProcessedCover {
        webp_data,
        fallback_data,
        fallback_format,
        original_width,
        original_height,
        final_width,
        final_height,
        was_resized,
        source,
    })
}

/// Load and process cover art from raw bytes.
///
/// Decodes the image data and processes it for upload.
pub fn process_cover_bytes(
    data: &[u8],
    source: CoverSource,
) -> Option<ProcessedCover> {
    let image = image::load_from_memory(data).ok()?;
    process_cover_image(&image, source)
}

/// Upload processed cover art to Archivist.
///
/// Uploads:
/// 1. WebP version (main)
/// 2. JPG or PNG version (fallback, format chosen based on image characteristics)
/// 3. Original high-res if it was resized (optional)
pub async fn upload_processed_cover(
    client: &Client,
    archivist_url: &str,
    cover: &ProcessedCover,
    original_data: Option<&[u8]>,
    base_filename: &str,
    auth: Option<&AuthCredentials>,
) -> Option<CoverUploadResult> {
    let upload_url = format!("{}/api/archivist/v1/data", archivist_url);

    // Generate filenames
    let base = base_filename
        .rsplit_once('.')
        .map(|(name, _)| name)
        .unwrap_or(base_filename);
    let webp_filename = format!("{}.webp", base);

    // Fallback filename and content type based on format
    let (fallback_filename, fallback_content_type) = match cover.fallback_format {
        FallbackFormat::Png => (format!("{}.png", base), "image/png"),
        FallbackFormat::Jpg => (format!("{}.jpg", base), "image/jpeg"),
    };

    // Upload WebP (main)
    let main_cid = upload_image_data(
        client,
        &upload_url,
        &cover.webp_data,
        "image/webp",
        &webp_filename,
        auth,
    ).await?;

    // Upload fallback (JPG or PNG)
    let fallback_cid = upload_image_data(
        client,
        &upload_url,
        &cover.fallback_data,
        fallback_content_type,
        &fallback_filename,
        auth,
    ).await?;

    // Upload high-res original if it was resized and we have the original data
    let highres_cid = if cover.was_resized {
        if let Some(data) = original_data {
            // Determine mime type and extension from data
            let (mime, ext) = if data.starts_with(&[0xFF, 0xD8]) {
                ("image/jpeg", "jpg")
            } else if data.starts_with(&[0x89, 0x50, 0x4E, 0x47]) {
                ("image/png", "png")
            } else if data.starts_with(b"GIF") {
                ("image/gif", "gif")
            } else if data.starts_with(b"RIFF") && data.len() > 12 && &data[8..12] == b"WEBP" {
                ("image/webp", "webp")
            } else {
                ("application/octet-stream", "bin")
            };
            let highres_filename = format!("{}_highres.{}", base, ext);
            upload_image_data(client, &upload_url, data, mime, &highres_filename, auth).await
        } else {
            None
        }
    } else {
        None
    };

    info!(
        main_cid = %main_cid,
        fallback_cid = %fallback_cid,
        fallback_format = ?cover.fallback_format,
        highres_cid = ?highres_cid,
        dimensions = format!("{}x{}", cover.final_width, cover.final_height),
        "Uploaded cover art"
    );

    Some(CoverUploadResult {
        main_cid,
        fallback_cid,
        fallback_format: cover.fallback_format,
        highres_cid,
        original_width: cover.original_width,
        original_height: cover.original_height,
        source: cover.source,
    })
}

/// Helper to upload image data to Archivist.
async fn upload_image_data(
    client: &Client,
    upload_url: &str,
    data: &[u8],
    content_type: &str,
    filename: &str,
    auth: Option<&AuthCredentials>,
) -> Option<String> {
    let mut request = client
        .post(upload_url)
        .header("Content-Type", content_type)
        .header(
            "Content-Disposition",
            format!("attachment; filename=\"{}\"", filename),
        )
        .body(data.to_vec());

    if let Some(creds) = auth {
        request = request
            .header("X-Pubkey", &creds.pubkey)
            .header("X-Timestamp", creds.timestamp.to_string())
            .header("X-Signature", &creds.signature);
    }

    let response = request.send().await.ok()?;
    if !response.status().is_success() {
        warn!(
            status = %response.status(),
            filename = %filename,
            "Failed to upload image"
        );
        return None;
    }

    let cid = response.text().await.ok()?.trim().to_string();
    Some(cid)
}

/// Upload image to Archivist as WebP.
async fn upload_webp(
    client: &Client,
    archivist_url: &str,
    image: &DynamicImage,
    filename: &str,
    auth: Option<&AuthCredentials>,
) -> Option<String> {
    // Encode WebP in a sync function to avoid Send issues with WebPMemory
    let webp_bytes = encode_webp(image)?;

    let upload_url = format!("{}/api/archivist/v1/data", archivist_url);

    // Generate WebP filename
    let webp_filename = filename
        .rsplit_once('.')
        .map(|(name, _)| format!("{}.webp", name))
        .unwrap_or_else(|| format!("{}.webp", filename));

    let mut request = client
        .post(&upload_url)
        .header("Content-Type", "image/webp")
        .header(
            "Content-Disposition",
            format!("attachment; filename=\"{}\"", webp_filename),
        )
        .body(webp_bytes);

    if let Some(creds) = auth {
        request = request
            .header("X-Pubkey", &creds.pubkey)
            .header("X-Timestamp", creds.timestamp.to_string())
            .header("X-Signature", &creds.signature);
    }

    let response = request.send().await.ok()?;
    if !response.status().is_success() {
        warn!(
            status = %response.status(),
            "Failed to upload cover art WebP"
        );
        return None;
    }

    let cid = response.text().await.ok()?.trim().to_string();
    Some(cid)
}

/// Find and process cover art from Archive.org item.
///
/// Strategy:
/// 1. Download __ia_thumb.jpg (Archive.org's auto-generated thumbnail)
/// 2. Compute its perceptual hash as reference
/// 3. Download top candidate images and compare hashes
/// 4. Select best match with highest similarity and largest size
/// 5. Convert to WebP and upload to Archivist
pub async fn find_and_upload_cover(
    client: &Client,
    archivist_url: &str,
    identifier: &str,
    candidates: &[super::import::CoverCandidate],
    auth: Option<&AuthCredentials>,
) -> CoverArtResult {
    let mut result = CoverArtResult {
        thumbnail_cid: None,
        candidates: Vec::new(),
        confident: false,
    };

    if candidates.is_empty() {
        debug!(identifier = %identifier, "No cover art candidates found");
        return result;
    }

    // 1. Try to download __ia_thumb.jpg as reference
    let reference_hash = match download_image(client, identifier, "__ia_thumb.jpg").await {
        Some(thumb) => {
            info!(identifier = %identifier, "Downloaded IA thumbnail for reference");
            Some(compute_hash(&thumb))
        }
        None => {
            debug!(identifier = %identifier, "No __ia_thumb.jpg found, using size-based selection");
            None
        }
    };

    // 2. Download and hash top candidates
    let mut matches: Vec<(CoverMatch, Option<DynamicImage>)> = Vec::new();

    for candidate in candidates.iter().take(MAX_CANDIDATES_TO_CHECK) {
        let image = match download_image(client, identifier, &candidate.filename).await {
            Some(img) => img,
            None => {
                debug!(filename = %candidate.filename, "Failed to download candidate");
                continue;
            }
        };

        let similarity = if let Some(ref ref_hash) = reference_hash {
            let candidate_hash = compute_hash(&image);
            let distance = ref_hash.dist(&candidate_hash);
            hamming_to_similarity(distance)
        } else {
            // No reference - use score from filename/size heuristics
            candidate.score.min(100)
        };

        debug!(
            filename = %candidate.filename,
            similarity = %similarity,
            size = %candidate.size,
            "Scored cover candidate"
        );

        matches.push((
            CoverMatch {
                filename: candidate.filename.clone(),
                similarity,
                size: candidate.size,
                cid: None,
            },
            Some(image),
        ));
    }

    if matches.is_empty() {
        warn!(identifier = %identifier, "No cover art candidates could be downloaded");
        return result;
    }

    // 3. Sort by similarity (primary) and size (secondary)
    matches.sort_by(|a, b| {
        b.0.similarity
            .cmp(&a.0.similarity)
            .then_with(|| b.0.size.cmp(&a.0.size))
    });

    // 4. Check if best match is confident
    let best = &matches[0];
    let confident = best.0.similarity >= MIN_SIMILARITY_THRESHOLD;

    info!(
        identifier = %identifier,
        filename = %best.0.filename,
        similarity = %best.0.similarity,
        confident = %confident,
        "Selected best cover art candidate"
    );

    // 5. Upload best match as WebP
    if let Some(ref image) = best.1 {
        if let Some(cid) = upload_webp(client, archivist_url, image, &best.0.filename, auth).await {
            info!(
                identifier = %identifier,
                cid = %cid,
                "Uploaded cover art as WebP"
            );
            result.thumbnail_cid = Some(cid.clone());

            // Update the match with the CID
            let mut updated_best = best.0.clone();
            updated_best.cid = Some(cid);
            result.candidates.push(updated_best);
        }
    }

    // Add remaining candidates (without CIDs) for admin review
    for (m, _) in matches.iter().skip(1) {
        result.candidates.push(m.clone());
    }

    result.confident = confident;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{Rgba, RgbaImage};

    #[test]
    fn test_hamming_to_similarity() {
        assert_eq!(hamming_to_similarity(0), 100); // Identical
        assert_eq!(hamming_to_similarity(64), 0); // Completely different
        assert_eq!(hamming_to_similarity(32), 50); // 50% similar
        assert_eq!(hamming_to_similarity(16), 75); // 75% similar
    }

    #[test]
    fn test_resize_to_max_no_upscale() {
        // Create a small 100x100 image
        let img = DynamicImage::ImageRgba8(RgbaImage::from_fn(100, 100, |_, _| {
            Rgba([255, 0, 0, 255])
        }));

        let (resized, was_resized) = resize_to_max(&img, 1024);

        // Should not upscale
        assert!(!was_resized);
        assert_eq!(resized.dimensions(), (100, 100));
    }

    #[test]
    fn test_resize_to_max_landscape() {
        // Create a 2000x1000 landscape image
        let img = DynamicImage::ImageRgba8(RgbaImage::from_fn(2000, 1000, |_, _| {
            Rgba([255, 0, 0, 255])
        }));

        let (resized, was_resized) = resize_to_max(&img, 1024);

        assert!(was_resized);
        let (w, h) = resized.dimensions();
        assert_eq!(w, 1024);
        assert!(h <= 1024); // Height should be proportionally smaller
        assert!(h > 500); // But still reasonable
    }

    #[test]
    fn test_resize_to_max_portrait() {
        // Create a 1000x2000 portrait image
        let img = DynamicImage::ImageRgba8(RgbaImage::from_fn(1000, 2000, |_, _| {
            Rgba([255, 0, 0, 255])
        }));

        let (resized, was_resized) = resize_to_max(&img, 1024);

        assert!(was_resized);
        let (w, h) = resized.dimensions();
        assert!(w <= 1024);
        assert_eq!(h, 1024);
    }

    #[test]
    fn test_should_use_png_with_transparency() {
        // Create an image with some transparent pixels
        let img = DynamicImage::ImageRgba8(RgbaImage::from_fn(100, 100, |x, _| {
            if x < 50 {
                Rgba([255, 0, 0, 255]) // Opaque
            } else {
                Rgba([0, 255, 0, 128]) // Semi-transparent
            }
        }));

        assert!(should_use_png(&img));
    }

    #[test]
    fn test_should_use_png_few_colors() {
        // Create an image with only 2 colors (like a logo)
        let img = DynamicImage::ImageRgba8(RgbaImage::from_fn(100, 100, |x, _| {
            if x % 2 == 0 {
                Rgba([255, 255, 255, 255]) // White
            } else {
                Rgba([0, 0, 0, 255]) // Black
            }
        }));

        assert!(should_use_png(&img));
    }

    #[test]
    fn test_should_use_jpg_photographic() {
        // Create an image with many colors (like a photo)
        let img = DynamicImage::ImageRgba8(RgbaImage::from_fn(100, 100, |x, y| {
            Rgba([
                (x * 2) as u8,
                (y * 2) as u8,
                ((x + y) % 256) as u8,
                255,
            ])
        }));

        // This should have many unique colors, so JPG is better
        assert!(!should_use_png(&img));
    }

    #[test]
    fn test_process_cover_image_basic() {
        // Create a simple test image
        let img = DynamicImage::ImageRgba8(RgbaImage::from_fn(500, 500, |x, y| {
            Rgba([
                (x % 256) as u8,
                (y % 256) as u8,
                ((x + y) % 256) as u8,
                255,
            ])
        }));

        let result = process_cover_image(&img, CoverSource::DirectoryFile);
        assert!(result.is_some());

        let cover = result.unwrap();
        assert!(!cover.webp_data.is_empty());
        assert!(!cover.fallback_data.is_empty());
        assert_eq!(cover.original_width, 500);
        assert_eq!(cover.original_height, 500);
        assert_eq!(cover.final_width, 500);
        assert_eq!(cover.final_height, 500);
        assert!(!cover.was_resized);
        assert_eq!(cover.source, CoverSource::DirectoryFile);
    }

    #[test]
    fn test_process_cover_image_needs_resize() {
        // Create a large image that needs resizing
        let img = DynamicImage::ImageRgba8(RgbaImage::from_fn(2000, 2000, |_, _| {
            Rgba([100, 100, 100, 255])
        }));

        let result = process_cover_image(&img, CoverSource::Embedded);
        assert!(result.is_some());

        let cover = result.unwrap();
        assert!(cover.was_resized);
        assert_eq!(cover.original_width, 2000);
        assert_eq!(cover.original_height, 2000);
        assert_eq!(cover.final_width, 1024);
        assert_eq!(cover.final_height, 1024);
    }

    #[test]
    fn test_fallback_format_selection() {
        // Transparent image should use PNG fallback
        let transparent_img = DynamicImage::ImageRgba8(RgbaImage::from_fn(100, 100, |_, _| {
            Rgba([255, 0, 0, 128]) // Semi-transparent
        }));
        let result = process_cover_image(&transparent_img, CoverSource::DirectoryFile);
        assert!(result.is_some());
        assert_eq!(result.unwrap().fallback_format, FallbackFormat::Png);

        // Photographic image should use JPG fallback
        let photo_img = DynamicImage::ImageRgba8(RgbaImage::from_fn(100, 100, |x, y| {
            Rgba([
                (x * 3 % 256) as u8,
                (y * 3 % 256) as u8,
                ((x * y) % 256) as u8,
                255,
            ])
        }));
        let result = process_cover_image(&photo_img, CoverSource::ArchiveOrg);
        assert!(result.is_some());
        assert_eq!(result.unwrap().fallback_format, FallbackFormat::Jpg);
    }
}
