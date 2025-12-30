//! Audio metadata extraction using lofty.
//!
//! Extracts ID3v2, Vorbis Comments, FLAC metadata, and embedded cover art
//! from audio files for Golden Format processing.

use std::io::Cursor;

use lofty::prelude::*;
use lofty::probe::Probe;
use lofty::picture::PictureType;
use tracing::{debug, warn};

/// Extracted audio metadata from tags.
#[derive(Debug, Clone, Default)]
pub struct AudioMetadata {
    /// Artist name (from ARTIST, TPE1, etc.)
    pub artist: Option<String>,
    /// Album name (from ALBUM, TALB, etc.)
    pub album: Option<String>,
    /// Release year
    pub year: Option<u32>,
    /// Genre
    pub genre: Option<String>,
    /// Track-level metadata
    pub tracks: Vec<TrackMetadata>,
}

/// Metadata for a single track.
#[derive(Debug, Clone)]
pub struct TrackMetadata {
    /// Track number (from TRACKNUMBER, TRCK, etc.)
    pub number: Option<u32>,
    /// Track title (from TITLE, TIT2, etc.)
    pub title: Option<String>,
    /// Track artist (if different from album artist)
    pub artist: Option<String>,
    /// Original filename
    pub filename: String,
}

/// Embedded cover art extracted from audio file.
#[derive(Debug, Clone)]
pub struct CoverArt {
    /// Raw image data
    pub data: Vec<u8>,
    /// MIME type (image/jpeg, image/png, etc.)
    pub mime_type: String,
    /// Picture type (front cover, back cover, etc.)
    pub picture_type: CoverArtType,
    /// Optional description
    pub description: Option<String>,
}

/// Type of cover art.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoverArtType {
    FrontCover,
    BackCover,
    Other,
}

impl From<PictureType> for CoverArtType {
    fn from(pt: PictureType) -> Self {
        match pt {
            PictureType::CoverFront => CoverArtType::FrontCover,
            PictureType::CoverBack => CoverArtType::BackCover,
            _ => CoverArtType::Other,
        }
    }
}

/// Extract metadata from audio file bytes.
///
/// Reads ID3v2, Vorbis Comments, or FLAC metadata depending on file format.
/// Returns album-level metadata (artist, album, year) from the first file's tags.
pub fn extract_audio_metadata(bytes: &[u8], filename: &str) -> Option<AudioMetadata> {
    let cursor = Cursor::new(bytes);

    let tagged_file = match Probe::new(cursor).guess_file_type() {
        Ok(probe) => match probe.read() {
            Ok(file) => file,
            Err(e) => {
                warn!(filename = %filename, error = %e, "Failed to read audio tags");
                return None;
            }
        },
        Err(e) => {
            warn!(filename = %filename, error = %e, "Failed to probe audio file type");
            return None;
        }
    };

    // Get primary tag (prefer ID3v2 > Vorbis > APE > ID3v1)
    let tag = tagged_file.primary_tag()
        .or_else(|| tagged_file.first_tag());

    let tag = match tag {
        Some(t) => t,
        None => {
            debug!(filename = %filename, "No tags found in audio file");
            return None;
        }
    };

    // Extract album-level metadata
    let artist = tag.artist().map(|s| s.to_string());
    let album = tag.album().map(|s| s.to_string());
    let year = tag.year();
    let genre = tag.genre().map(|s| s.to_string());

    // Extract track-level metadata
    let track_number = tag.track().and_then(|t| if t > 0 { Some(t) } else { None });
    let title = tag.title().map(|s| s.to_string());
    let track_artist = tag.get_string(&ItemKey::TrackArtist).map(|s| s.to_string());

    let track = TrackMetadata {
        number: track_number,
        title,
        artist: track_artist,
        filename: filename.to_string(),
    };

    debug!(
        filename = %filename,
        artist = ?artist,
        album = ?album,
        year = ?year,
        track_number = ?track_number,
        "Extracted audio metadata"
    );

    Some(AudioMetadata {
        artist,
        album,
        year,
        genre,
        tracks: vec![track],
    })
}

/// Extract track metadata only (for subsequent files after first).
///
/// Use this for files after the first to get track-specific info
/// without re-extracting album metadata.
pub fn extract_track_metadata(bytes: &[u8], filename: &str) -> Option<TrackMetadata> {
    let cursor = Cursor::new(bytes);

    let tagged_file = match Probe::new(cursor).guess_file_type() {
        Ok(probe) => probe.read().ok()?,
        Err(_) => return None,
    };

    let tag = tagged_file.primary_tag()
        .or_else(|| tagged_file.first_tag())?;

    let track_number = tag.track().and_then(|t| if t > 0 { Some(t) } else { None });
    let title = tag.title().map(|s| s.to_string());
    let artist = tag.get_string(&ItemKey::TrackArtist).map(|s| s.to_string());

    Some(TrackMetadata {
        number: track_number,
        title,
        artist,
        filename: filename.to_string(),
    })
}

/// Extract embedded cover art from audio file.
///
/// Prefers front cover, falls back to any available picture.
/// Returns the largest image if multiple are present.
pub fn extract_embedded_cover(bytes: &[u8], filename: &str) -> Option<CoverArt> {
    let cursor = Cursor::new(bytes);

    let tagged_file = match Probe::new(cursor).guess_file_type() {
        Ok(probe) => probe.read().ok()?,
        Err(_) => return None,
    };

    let tag = tagged_file.primary_tag()
        .or_else(|| tagged_file.first_tag())?;

    let pictures = tag.pictures();
    if pictures.is_empty() {
        return None;
    }

    // Find best picture: prefer front cover, then largest
    let best_picture = pictures
        .iter()
        .max_by(|a, b| {
            // Front cover gets priority
            let a_front = matches!(a.pic_type(), PictureType::CoverFront);
            let b_front = matches!(b.pic_type(), PictureType::CoverFront);

            match (a_front, b_front) {
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                _ => a.data().len().cmp(&b.data().len()),
            }
        })?;

    let mime_type = best_picture.mime_type()
        .map(|m| m.to_string())
        .unwrap_or_else(|| "image/jpeg".to_string());

    debug!(
        filename = %filename,
        mime_type = %mime_type,
        size = best_picture.data().len(),
        "Extracted embedded cover art"
    );

    Some(CoverArt {
        data: best_picture.data().to_vec(),
        mime_type,
        picture_type: best_picture.pic_type().into(),
        description: best_picture.description().map(|s| s.to_string()),
    })
}

/// Merge metadata from multiple tracks into a single AudioMetadata.
///
/// Takes the first non-None values for album-level fields,
/// and collects all track metadata.
pub fn merge_track_metadata(
    album_meta: AudioMetadata,
    additional_tracks: Vec<TrackMetadata>,
) -> AudioMetadata {
    let mut tracks = album_meta.tracks;
    tracks.extend(additional_tracks);

    // Sort by track number
    tracks.sort_by_key(|t| t.number.unwrap_or(999));

    AudioMetadata {
        tracks,
        ..album_meta
    }
}

/// Determine the most common artist from a list of tracks.
///
/// Used when tracks have different artists to find the album artist,
/// or returns "Various Artists" if too diverse.
pub fn determine_album_artist(tracks: &[TrackMetadata]) -> Option<String> {
    use std::collections::HashMap;

    let mut artist_counts: HashMap<&str, usize> = HashMap::new();

    for track in tracks {
        if let Some(ref artist) = track.artist {
            *artist_counts.entry(artist.as_str()).or_insert(0) += 1;
        }
    }

    if artist_counts.is_empty() {
        return None;
    }

    let total_tracks = tracks.len();
    let (most_common, count) = artist_counts
        .into_iter()
        .max_by_key(|(_, count)| *count)?;

    // If most common artist appears in less than 50% of tracks, it's "Various Artists"
    if count * 2 < total_tracks {
        Some("Various Artists".to_string())
    } else {
        Some(most_common.to_string())
    }
}

// =============================================================================
// Golden Format Functions
// =============================================================================

/// Characters that are unsafe in filenames across platforms.
const UNSAFE_CHARS: &[char] = &['/', '\\', ':', '*', '?', '"', '<', '>', '|'];

/// Sanitize a string for use in filenames.
///
/// Replaces unsafe filesystem characters with underscores.
/// Preserves Unicode characters - only replaces dangerous ones.
pub fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| {
            if UNSAFE_CHARS.contains(&c) {
                '_'
            } else {
                c
            }
        })
        .collect::<String>()
        .trim()
        .to_string()
}

/// Generate a Golden Format filename for a track.
///
/// Format: `01 - Track Title.ext`
/// - Track number is zero-padded to 2 digits
/// - Title is sanitized for filesystem safety
/// - Extension is preserved from original filename
///
/// # Arguments
/// * `track_num` - Optional track number (if None, no prefix is added)
/// * `title` - Track title (will be sanitized)
/// * `original_filename` - Original filename to extract extension from
pub fn generate_golden_filename(
    track_num: Option<u32>,
    title: &str,
    original_filename: &str,
) -> String {
    // Extract extension from original filename
    let extension = original_filename
        .rfind('.')
        .map(|i| &original_filename[i..])
        .unwrap_or("");

    let clean_title = sanitize_filename(title);

    if let Some(num) = track_num {
        format!("{:02} - {}{}", num, clean_title, extension)
    } else {
        format!("{}{}", clean_title, extension)
    }
}

/// Generate a Golden Format directory name for an album.
///
/// Format: `Artist - Album (Year)`
/// - Artist and album are sanitized for filesystem safety
/// - Year is optional (omitted if None)
/// - Falls back to just album name if artist is missing
///
/// # Arguments
/// * `artist` - Optional artist name
/// * `album` - Album name (required)
/// * `year` - Optional release year
pub fn generate_golden_dirname(
    artist: Option<&str>,
    album: &str,
    year: Option<u32>,
) -> String {
    let clean_album = sanitize_filename(album);

    let base = match artist {
        Some(a) => {
            let clean_artist = sanitize_filename(a);
            format!("{} - {}", clean_artist, clean_album)
        }
        None => clean_album,
    };

    match year {
        Some(y) => format!("{} ({})", base, y),
        None => base,
    }
}

/// Extract extension from a filename.
pub fn get_extension(filename: &str) -> Option<&str> {
    filename.rfind('.').map(|i| &filename[i..])
}

/// Check if a filename looks like an audio file.
pub fn is_audio_file(filename: &str) -> bool {
    let lower = filename.to_lowercase();
    lower.ends_with(".mp3")
        || lower.ends_with(".flac")
        || lower.ends_with(".ogg")
        || lower.ends_with(".opus")
        || lower.ends_with(".m4a")
        || lower.ends_with(".aac")
        || lower.ends_with(".wav")
        || lower.ends_with(".aiff")
        || lower.ends_with(".wma")
}

/// Check if a filename looks like a cover image.
///
/// Prioritizes common cover art filenames.
pub fn is_cover_image(filename: &str) -> bool {
    let lower = filename.to_lowercase();

    // Must be an image
    if !lower.ends_with(".jpg")
        && !lower.ends_with(".jpeg")
        && !lower.ends_with(".png")
        && !lower.ends_with(".gif")
        && !lower.ends_with(".webp")
    {
        return false;
    }

    true
}

/// Score a cover image candidate by filename.
///
/// Higher scores = more likely to be the primary cover art.
/// Returns 0 for non-image files.
pub fn score_cover_filename(filename: &str) -> u32 {
    let lower = filename.to_lowercase();

    // Must be an image
    if !is_cover_image(filename) {
        return 0;
    }

    // Priority names get high scores
    let priority_names = [
        ("cover.", 100),
        ("front.", 95),
        ("folder.", 90),
        ("album.", 85),
        ("artwork.", 80),
        ("[cover]", 75),
        ("albumart", 70),
    ];

    for (pattern, score) in priority_names {
        if lower.contains(pattern) {
            return score;
        }
    }

    // Skip obvious non-cover images
    if lower.contains("back")
        || lower.contains("booklet")
        || lower.contains("inlay")
        || lower.contains("disc")
        || lower.contains("cd")
    {
        return 10;
    }

    // Generic image gets base score
    50
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cover_art_type_from_picture_type() {
        assert_eq!(CoverArtType::from(PictureType::CoverFront), CoverArtType::FrontCover);
        assert_eq!(CoverArtType::from(PictureType::CoverBack), CoverArtType::BackCover);
        assert_eq!(CoverArtType::from(PictureType::Other), CoverArtType::Other);
    }

    #[test]
    fn test_determine_album_artist_single() {
        let tracks = vec![
            TrackMetadata {
                number: Some(1),
                title: Some("Track 1".to_string()),
                artist: Some("Artist A".to_string()),
                filename: "01.mp3".to_string(),
            },
            TrackMetadata {
                number: Some(2),
                title: Some("Track 2".to_string()),
                artist: Some("Artist A".to_string()),
                filename: "02.mp3".to_string(),
            },
        ];

        assert_eq!(determine_album_artist(&tracks), Some("Artist A".to_string()));
    }

    #[test]
    fn test_determine_album_artist_various() {
        let tracks = vec![
            TrackMetadata {
                number: Some(1),
                title: Some("Track 1".to_string()),
                artist: Some("Artist A".to_string()),
                filename: "01.mp3".to_string(),
            },
            TrackMetadata {
                number: Some(2),
                title: Some("Track 2".to_string()),
                artist: Some("Artist B".to_string()),
                filename: "02.mp3".to_string(),
            },
            TrackMetadata {
                number: Some(3),
                title: Some("Track 3".to_string()),
                artist: Some("Artist C".to_string()),
                filename: "03.mp3".to_string(),
            },
        ];

        assert_eq!(determine_album_artist(&tracks), Some("Various Artists".to_string()));
    }

    // Golden Format tests

    #[test]
    fn test_sanitize_filename() {
        assert_eq!(sanitize_filename("Normal Title"), "Normal Title");
        assert_eq!(sanitize_filename("Title: Subtitle"), "Title_ Subtitle");
        assert_eq!(sanitize_filename("What?"), "What_");
        assert_eq!(sanitize_filename("A/B\\C"), "A_B_C");
        assert_eq!(sanitize_filename("Test <> \"quotes\""), "Test __ _quotes_");
        // Unicode preserved
        assert_eq!(sanitize_filename("日本語タイトル"), "日本語タイトル");
        assert_eq!(sanitize_filename("Ñoño"), "Ñoño");
    }

    #[test]
    fn test_generate_golden_filename() {
        assert_eq!(
            generate_golden_filename(Some(1), "Track Title", "original.mp3"),
            "01 - Track Title.mp3"
        );
        assert_eq!(
            generate_golden_filename(Some(12), "Another Song", "file.flac"),
            "12 - Another Song.flac"
        );
        assert_eq!(
            generate_golden_filename(None, "No Number", "track.ogg"),
            "No Number.ogg"
        );
        // Sanitization
        assert_eq!(
            generate_golden_filename(Some(3), "What: The Remix?", "song.mp3"),
            "03 - What_ The Remix_.mp3"
        );
    }

    #[test]
    fn test_generate_golden_dirname() {
        assert_eq!(
            generate_golden_dirname(Some("Artist"), "Album", Some(2024)),
            "Artist - Album (2024)"
        );
        assert_eq!(
            generate_golden_dirname(Some("Artist"), "Album", None),
            "Artist - Album"
        );
        assert_eq!(
            generate_golden_dirname(None, "Album Only", Some(2020)),
            "Album Only (2020)"
        );
        assert_eq!(
            generate_golden_dirname(None, "Album Only", None),
            "Album Only"
        );
        // Sanitization
        assert_eq!(
            generate_golden_dirname(Some("Artist: Name"), "Album/Title", Some(2023)),
            "Artist_ Name - Album_Title (2023)"
        );
    }

    #[test]
    fn test_is_audio_file() {
        assert!(is_audio_file("track.mp3"));
        assert!(is_audio_file("Track.FLAC"));
        assert!(is_audio_file("song.ogg"));
        assert!(is_audio_file("audio.opus"));
        assert!(!is_audio_file("cover.jpg"));
        assert!(!is_audio_file("readme.txt"));
    }

    #[test]
    fn test_is_cover_image() {
        assert!(is_cover_image("cover.jpg"));
        assert!(is_cover_image("folder.png"));
        assert!(is_cover_image("artwork.webp"));
        assert!(!is_cover_image("track.mp3"));
        assert!(!is_cover_image("readme.txt"));
    }

    #[test]
    fn test_score_cover_filename() {
        assert_eq!(score_cover_filename("cover.jpg"), 100);
        assert_eq!(score_cover_filename("front.png"), 95);
        assert_eq!(score_cover_filename("folder.jpg"), 90);
        assert_eq!(score_cover_filename("album.jpg"), 85);
        assert_eq!(score_cover_filename("[cover] Artist - Title.jpg"), 75);
        assert_eq!(score_cover_filename("random_image.jpg"), 50);
        assert_eq!(score_cover_filename("back.jpg"), 10);
        assert_eq!(score_cover_filename("track.mp3"), 0);
    }
}
