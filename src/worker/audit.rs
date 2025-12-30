//! Audit executor for quality ladder compliance.
//!
//! Scans releases in Citadel Lens and detects quality issues:
//! - Missing audio quality metadata
//! - Missing license, source, description, year
//! - Missing quality ladder encodes (FLAC without Opus)
//! - Missing cover art
//! - Content not on Archivist
//! - Broken releases (no files)

use std::collections::HashMap;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn, debug};

use crate::crdt::{AuditIssue, ReleaseAudit};

/// Release from Citadel Lens API.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LensRelease {
    pub id: String,
    pub name: String,
    #[serde(rename = "postedBy")]
    pub posted_by: Option<String>,
    pub description: Option<String>,
    pub year: Option<u32>,
    #[serde(rename = "contentCID")]
    pub content_cid: Option<String>,
    #[serde(rename = "thumbnailCID")]
    pub thumbnail_cid: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub status: Option<String>,
}


/// The ideal quality ladder tiers (in priority order).
const IDEAL_QUALITY_LADDER: &[&str] = &[
    "lossless",    // FLAC/WAV
    "opus",        // Opus (generic)
    "mp3_320",     // MP3 320 CBR
    "mp3_v0",      // MP3 V0 VBR
    "ogg",         // Ogg Vorbis
];

/// Tiers that should be generated from lossless source.
const OPUS_LADDER_TIERS: &[&str] = &[
    "opus_192",
    "opus_160",
    "opus_128",
    "opus_96",
    "opus_64",
];

/// Fetch all approved releases from Citadel Lens.
pub async fn fetch_releases(
    client: &Client,
    lens_url: &str,
) -> anyhow::Result<Vec<LensRelease>> {
    let url = format!("{}/api/v1/releases?status=approved&limit=1000", lens_url.trim_end_matches('/'));

    debug!(url = %url, "Fetching releases from Citadel Lens");

    let response = client
        .get(&url)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Failed to fetch releases: {} - {}", status, body);
    }

    // API returns array directly
    let releases: Vec<LensRelease> = response.json().await?;

    info!(count = releases.len(), "Fetched releases from Citadel Lens");

    Ok(releases)
}

/// Check if a CID is an Archivist CID (starts with zD or zE).
fn is_archivist_cid(cid: &str) -> bool {
    cid.starts_with("zD") || cid.starts_with("zE")
}

/// Check if a CID is an IPFS CID (starts with Qm or bafy).
fn is_ipfs_cid(cid: &str) -> bool {
    cid.starts_with("Qm") || cid.starts_with("bafy")
}

/// Check if a release is actually an Artist entry (not a real release).
/// Artists are stored in releases table but have metadata.type = "artist".
fn is_artist_entry(release: &LensRelease) -> bool {
    release.metadata
        .as_ref()
        .and_then(|m| m.get("type"))
        .and_then(|t| t.as_str())
        .map(|t| t == "artist")
        .unwrap_or(false)
}

/// Audit a single release for quality issues.
pub fn audit_release(release: &LensRelease) -> ReleaseAudit {
    let mut issues = Vec::new();
    let mut available_tiers = Vec::new();
    let mut missing_tiers = Vec::new();

    // Parse metadata
    let metadata = release.metadata.as_ref();

    // Extract fields from metadata
    let audio_quality = metadata.and_then(|m| m.get("audioQuality"));
    let quality_ladder = metadata.and_then(|m| m.get("qualityLadder"));
    let license = metadata.and_then(|m| m.get("license"));
    let source = metadata.and_then(|m| m.get("source"));
    let track_metadata = metadata.and_then(|m| m.get("trackMetadata"));
    let archive_org_id = metadata
        .and_then(|m| m.get("archiveOrgId"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let import_type = metadata
        .and_then(|m| m.get("importType"))
        .and_then(|v| v.as_str());

    // Detect source quality from audioQuality metadata
    let source_quality = audio_quality
        .and_then(|q| q.get("format"))
        .and_then(|f| f.as_str())
        .map(|s| s.to_string());

    // Check for missing audio quality
    if audio_quality.is_none() {
        issues.push(AuditIssue::MissingAudioQuality);
    }

    // Check for missing license
    if license.is_none() {
        issues.push(AuditIssue::MissingLicense);
    }

    // Check for missing source (unless it's "Unknown" or "Self")
    match source.and_then(|s| s.as_str()) {
        None => {
            issues.push(AuditIssue::MissingSource);
        }
        Some(s) if s.to_lowercase() != "unknown" && s.to_lowercase() != "self" => {
            // Has a valid source, good
        }
        Some(_) => {
            // Marked as Unknown or Self, acceptable
        }
    }

    // Check for missing description
    if release.description.is_none() || release.description.as_ref().map(|s| s.trim().is_empty()).unwrap_or(true) {
        issues.push(AuditIssue::MissingDescription);
    }

    // Check for missing year
    if release.year.is_none() {
        issues.push(AuditIssue::MissingYear);
    }

    // Check for missing cover art
    if release.thumbnail_cid.is_none() {
        issues.push(AuditIssue::MissingCoverArt);
    }

    // Check for missing track metadata
    if track_metadata.is_none() {
        issues.push(AuditIssue::MissingTrackMetadata);
    }

    // Check content CID
    match &release.content_cid {
        None => {
            issues.push(AuditIssue::InvalidContentCid);
        }
        Some(cid) if cid.is_empty() => {
            issues.push(AuditIssue::InvalidContentCid);
        }
        Some(cid) if is_ipfs_cid(cid) && !is_archivist_cid(cid) => {
            // Content is on IPFS but not Archivist
            issues.push(AuditIssue::NotOnArchivist);
        }
        Some(_) => {
            // Valid CID
        }
    }

    // Check quality ladder for missing Opus encodes
    if let Some(ladder) = quality_ladder.and_then(|l| l.as_object()) {
        // Collect available tiers
        for (tier, _cid) in ladder.iter() {
            available_tiers.push(tier.clone());
        }

        // If we have lossless, check for Opus encodes
        let has_lossless = available_tiers.iter().any(|t| t == "lossless");
        if has_lossless {
            // Check which Opus tiers are missing
            let has_any_opus = available_tiers.iter().any(|t| t.starts_with("opus"));
            if !has_any_opus {
                issues.push(AuditIssue::MissingOpusEncodes);
                // Add expected Opus tiers to missing
                for tier in OPUS_LADDER_TIERS {
                    missing_tiers.push(tier.to_string());
                }
            }
        }
    } else if audio_quality.is_some() {
        // Has audio quality but no quality ladder - single tier available
        if let Some(format) = source_quality.as_ref() {
            if format == "flac" || format == "wav" {
                available_tiers.push("lossless".to_string());
                // Lossless without Opus ladder
                issues.push(AuditIssue::MissingOpusEncodes);
                for tier in OPUS_LADDER_TIERS {
                    missing_tiers.push(tier.to_string());
                }
            } else {
                available_tiers.push(format.clone());
            }
        }
    }

    // Check if this is an Archive.org import that could be refetched
    if import_type == Some("archive.org") {
        if let Some(ref id) = archive_org_id {
            // Could potentially refetch for:
            // - Missing cover art
            // - Missing metadata
            // - Better quality source
            let could_benefit = issues.iter().any(|i| matches!(i,
                AuditIssue::MissingCoverArt |
                AuditIssue::MissingDescription |
                AuditIssue::MissingYear |
                AuditIssue::MissingTrackMetadata
            ));

            if could_benefit {
                issues.push(AuditIssue::CanRefetchFromArchive { identifier: id.clone() });
            }
        }
    }

    // TODO: Check for credits/attribution (need to define what this means)
    // TODO: Check for unused track art (need to analyze actual audio files)
    // TODO: Check for no audio files (need to query Archivist directory)

    ReleaseAudit {
        release_id: release.id.clone(),
        title: release.name.clone(),
        artist: release.posted_by.clone(),
        source_quality,
        available_tiers,
        missing_tiers,
        issues,
        archive_org_id,
        content_cid: release.content_cid.clone(),
    }
}

/// Run a full audit of all releases.
pub async fn run_audit(
    client: &Client,
    lens_url: &str,
    on_progress: impl Fn(f32, Option<String>),
) -> anyhow::Result<(usize, usize, Vec<ReleaseAudit>, HashMap<String, usize>)> {
    on_progress(0.0, Some("Fetching releases from Citadel Lens...".to_string()));

    let all_releases = fetch_releases(client, lens_url).await?;

    // Filter out Artist entries - they're stored in releases but aren't actual releases
    let releases: Vec<_> = all_releases.into_iter()
        .filter(|r| !is_artist_entry(r))
        .collect();

    let total = releases.len();

    if total == 0 {
        return Ok((0, 0, vec![], HashMap::new()));
    }

    on_progress(0.1, Some(format!("Auditing {} releases (excluding artists)...", total)));

    let mut audits_with_issues = Vec::new();
    let mut issue_counts: HashMap<String, usize> = HashMap::new();

    for (i, release) in releases.iter().enumerate() {
        let audit = audit_release(release);

        // Count issues
        for issue in &audit.issues {
            let key = format!("{:?}", issue).split('{').next().unwrap_or("Unknown").trim().to_string();
            *issue_counts.entry(key).or_insert(0) += 1;
        }

        // Only keep audits with issues
        if !audit.issues.is_empty() {
            audits_with_issues.push(audit);
        }

        // Update progress every 10 releases
        if i % 10 == 0 {
            let progress = 0.1 + 0.9 * (i as f32 / total as f32);
            on_progress(progress, Some(format!("Audited {}/{} releases", i + 1, total)));
        }
    }

    let releases_with_issues = audits_with_issues.len();

    info!(
        total = total,
        with_issues = releases_with_issues,
        "Audit complete"
    );

    on_progress(1.0, Some(format!(
        "Audit complete: {}/{} releases have issues",
        releases_with_issues, total
    )));

    Ok((total, releases_with_issues, audits_with_issues, issue_counts))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_release_no_metadata() {
        let release = LensRelease {
            id: "test-123".to_string(),
            name: "Test Album".to_string(),
            posted_by: None,
            description: None,
            year: None,
            content_cid: None,
            thumbnail_cid: None,
            metadata: None,
            status: Some("approved".to_string()),
        };

        let audit = audit_release(&release);

        assert!(audit.issues.contains(&AuditIssue::MissingAudioQuality));
        assert!(audit.issues.contains(&AuditIssue::MissingLicense));
        assert!(audit.issues.contains(&AuditIssue::MissingSource));
        assert!(audit.issues.contains(&AuditIssue::MissingDescription));
        assert!(audit.issues.contains(&AuditIssue::MissingYear));
        assert!(audit.issues.contains(&AuditIssue::MissingCoverArt));
        assert!(audit.issues.contains(&AuditIssue::InvalidContentCid));
    }

    #[test]
    fn test_audit_release_with_ipfs_cid() {
        let release = LensRelease {
            id: "test-123".to_string(),
            name: "Test Album".to_string(),
            posted_by: Some("Artist".to_string()),
            description: Some("A great album".to_string()),
            year: Some(2024),
            content_cid: Some("QmXyz123".to_string()),  // IPFS, not Archivist
            thumbnail_cid: Some("QmThumb".to_string()),
            metadata: Some(serde_json::json!({
                "audioQuality": { "format": "flac" },
                "license": { "type": "cc-by" },
                "source": "archive.org:test",
                "trackMetadata": "[{\"title\": \"Track 1\"}]"
            })),
            status: Some("approved".to_string()),
        };

        let audit = audit_release(&release);

        assert!(audit.issues.contains(&AuditIssue::NotOnArchivist));
        assert!(audit.issues.contains(&AuditIssue::MissingOpusEncodes));
        assert_eq!(audit.source_quality, Some("flac".to_string()));
    }

    #[test]
    fn test_archivist_cid_detection() {
        assert!(is_archivist_cid("zDXyz123"));
        assert!(is_archivist_cid("zEabc456"));
        assert!(!is_archivist_cid("QmXyz123"));
        assert!(!is_archivist_cid("bafyxyz"));
    }
}
