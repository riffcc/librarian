//! Cover art detection and processing for Archive.org imports.
//!
//! Uses perceptual hashing to find high-resolution versions of cover art:
//! 1. Downloads Archive.org's __ia_thumb.jpg as reference
//! 2. Computes perceptual hash of the thumbnail
//! 3. Downloads candidate images and compares hashes
//! 4. Selects best match based on similarity and resolution
//! 5. Converts to WebP at 85% quality for storage efficiency

use image::DynamicImage;
use image_hasher::{HasherConfig, ImageHash};
use reqwest::Client;
use serde::Serialize;
use tracing::{debug, info, warn};

use crate::auth::AuthCredentials;

/// Minimum similarity score (0-100) to consider a match.
/// Lower Hamming distance = higher similarity.
/// Hash size is 64 bits, so max distance is 64.
const MIN_SIMILARITY_THRESHOLD: u32 = 70;

/// Maximum number of candidate images to download and compare.
const MAX_CANDIDATES_TO_CHECK: usize = 5;

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

/// Encode image to WebP at 85% quality.
/// Returns None if encoding fails.
fn encode_webp(image: &DynamicImage) -> Option<Vec<u8>> {
    let encoder = webp::Encoder::from_image(image).ok()?;
    let webp_data = encoder.encode(85.0);
    Some(webp_data.to_vec())
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

    #[test]
    fn test_hamming_to_similarity() {
        assert_eq!(hamming_to_similarity(0), 100); // Identical
        assert_eq!(hamming_to_similarity(64), 0); // Completely different
        assert_eq!(hamming_to_similarity(32), 50); // 50% similar
        assert_eq!(hamming_to_similarity(16), 75); // 75% similar
    }
}
