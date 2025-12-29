//! Archive.org browsing and proxy handlers.

use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::api::ApiState;

/// Search query parameters.
#[derive(Deserialize)]
pub struct SearchQuery {
    /// Search query string.
    pub q: String,

    /// Page number (1-indexed).
    #[serde(default = "default_page")]
    pub page: u32,

    /// Results per page.
    #[serde(default = "default_rows")]
    pub rows: u32,
}

fn default_page() -> u32 {
    1
}

fn default_rows() -> u32 {
    50
}

/// Pagination query parameters (for collection browsing).
#[derive(Deserialize)]
pub struct PaginationQuery {
    /// Page number (1-indexed).
    #[serde(default = "default_page")]
    pub page: u32,

    /// Results per page.
    #[serde(default = "default_rows")]
    pub rows: u32,
}

/// Search result item.
#[derive(Serialize)]
pub struct SearchResultItem {
    /// Archive.org identifier.
    pub identifier: String,

    /// Item title.
    pub title: Option<String>,

    /// Creator/artist.
    pub creator: Option<String>,

    /// Media type.
    pub mediatype: Option<String>,

    /// Description.
    pub description: Option<String>,

    /// Date published.
    pub date: Option<String>,

    /// Thumbnail URL (proxied through Librarian).
    pub thumbnail_url: String,
}

/// Search results response.
#[derive(Serialize)]
pub struct SearchResponse {
    /// Total number of results.
    pub total: usize,

    /// Current page.
    pub page: u32,

    /// Results per page.
    pub rows: u32,

    /// Result items.
    pub items: Vec<SearchResultItem>,
}

/// Archive.org advanced search API response.
#[derive(Deserialize)]
struct ArchiveSearchResponse {
    response: ArchiveSearchResponseInner,
}

#[derive(Deserialize)]
struct ArchiveSearchResponseInner {
    #[serde(rename = "numFound")]
    num_found: usize,
    docs: Vec<ArchiveSearchDoc>,
}

#[derive(Deserialize)]
struct ArchiveSearchDoc {
    identifier: String,
    title: Option<String>,
    creator: Option<serde_json::Value>,
    mediatype: Option<String>,
    description: Option<serde_json::Value>,
    date: Option<String>,
}

/// Search Archive.org items.
pub async fn search_items(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<SearchResponse>, (StatusCode, String)> {
    let url = format!(
        "https://archive.org/advancedsearch.php?q={}&output=json&rows={}&page={}",
        urlencoding::encode(&query.q),
        query.rows,
        query.page
    );

    let response = state
        .http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("Failed to query Archive.org: {}", e)))?;

    if !response.status().is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("Archive.org returned status {}", response.status()),
        ));
    }

    let data: ArchiveSearchResponse = response
        .json()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("Failed to parse response: {}", e)))?;

    let items: Vec<SearchResultItem> = data
        .response
        .docs
        .into_iter()
        .map(|doc| {
            let creator = match doc.creator {
                Some(serde_json::Value::String(s)) => Some(s),
                Some(serde_json::Value::Array(arr)) => arr
                    .first()
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                _ => None,
            };

            let description = match doc.description {
                Some(serde_json::Value::String(s)) => Some(s),
                Some(serde_json::Value::Array(arr)) => arr
                    .first()
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                _ => None,
            };

            SearchResultItem {
                thumbnail_url: format!("/api/v1/archive/items/{}/thumbnail", doc.identifier),
                identifier: doc.identifier,
                title: doc.title,
                creator,
                mediatype: doc.mediatype,
                description,
                date: doc.date,
            }
        })
        .collect();

    Ok(Json(SearchResponse {
        total: data.response.num_found,
        page: query.page,
        rows: query.rows,
        items,
    }))
}

/// Get collection details and items.
pub async fn get_collection(
    State(state): State<Arc<ApiState>>,
    Path(collection_id): Path<String>,
    Query(query): Query<PaginationQuery>,
) -> Result<Json<SearchResponse>, (StatusCode, String)> {
    // Search within the collection
    let url = format!(
        "https://archive.org/advancedsearch.php?q=collection:({})&output=json&rows={}&page={}",
        urlencoding::encode(&collection_id),
        query.rows,
        query.page
    );

    let response = state
        .http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("Failed to query Archive.org: {}", e)))?;

    if !response.status().is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("Archive.org returned status {}", response.status()),
        ));
    }

    let data: ArchiveSearchResponse = response
        .json()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("Failed to parse response: {}", e)))?;

    let items: Vec<SearchResultItem> = data
        .response
        .docs
        .into_iter()
        .map(|doc| {
            let creator = match doc.creator {
                Some(serde_json::Value::String(s)) => Some(s),
                Some(serde_json::Value::Array(arr)) => arr
                    .first()
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                _ => None,
            };

            SearchResultItem {
                thumbnail_url: format!("/api/v1/archive/items/{}/thumbnail", doc.identifier),
                identifier: doc.identifier,
                title: doc.title,
                creator,
                mediatype: doc.mediatype,
                description: None,
                date: doc.date,
            }
        })
        .collect();

    Ok(Json(SearchResponse {
        total: data.response.num_found,
        page: query.page,
        rows: query.rows,
        items,
    }))
}

/// Get ALL items in a collection (server-side pagination).
/// This fetches all pages from Archive.org and returns them in one response.
pub async fn get_collection_all(
    State(state): State<Arc<ApiState>>,
    Path(collection_id): Path<String>,
) -> Result<Json<SearchResponse>, (StatusCode, String)> {
    let mut all_items: Vec<SearchResultItem> = Vec::new();
    let mut page = 1u32;
    let rows = 100u32; // Fetch 100 at a time for efficiency
    let mut total: usize = 0;

    loop {
        let url = format!(
            "https://archive.org/advancedsearch.php?q=collection:({})&output=json&rows={}&page={}",
            urlencoding::encode(&collection_id),
            rows,
            page
        );

        let response = state
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, format!("Failed to query Archive.org: {}", e)))?;

        if !response.status().is_success() {
            return Err((
                StatusCode::BAD_GATEWAY,
                format!("Archive.org returned status {}", response.status()),
            ));
        }

        let data: ArchiveSearchResponse = response
            .json()
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, format!("Failed to parse response: {}", e)))?;

        total = data.response.num_found;

        if data.response.docs.is_empty() {
            break;
        }

        for doc in data.response.docs {
            let creator = match doc.creator {
                Some(serde_json::Value::String(s)) => Some(s),
                Some(serde_json::Value::Array(arr)) => arr
                    .first()
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                _ => None,
            };

            all_items.push(SearchResultItem {
                thumbnail_url: format!("/api/v1/archive/items/{}/thumbnail", doc.identifier),
                identifier: doc.identifier,
                title: doc.title,
                creator,
                mediatype: doc.mediatype,
                description: None,
                date: doc.date,
            });
        }

        // Check if we've fetched all items
        if all_items.len() >= total {
            break;
        }

        page += 1;
    }

    Ok(Json(SearchResponse {
        total,
        page: 1,
        rows: all_items.len() as u32,
        items: all_items,
    }))
}

/// Item metadata response.
#[derive(Serialize)]
pub struct ItemResponse {
    /// Archive.org identifier.
    pub identifier: String,

    /// Item metadata.
    pub metadata: ItemMetadata,

    /// Files in the item.
    pub files: Vec<ItemFile>,
}

#[derive(Serialize)]
pub struct ItemMetadata {
    pub title: Option<String>,
    pub creator: Option<String>,
    pub date: Option<String>,
    pub description: Option<String>,
    pub mediatype: Option<String>,
    pub collection: Option<Vec<String>>,
}

#[derive(Serialize)]
pub struct ItemFile {
    pub name: String,
    pub format: Option<String>,
    pub size: Option<String>,
    pub source: Option<String>,
    pub stream_url: String,
    // Rich metadata from ID3/Vorbis tags
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub track: Option<String>,
    pub length: Option<String>,
    pub genre: Option<String>,
}

/// Archive.org metadata API response.
#[derive(Deserialize)]
struct ArchiveMetadataResponse {
    metadata: ArchiveItemMetadata,
    files: Vec<ArchiveFileEntry>,
}

#[derive(Deserialize)]
struct ArchiveItemMetadata {
    identifier: String,
    title: Option<serde_json::Value>,
    creator: Option<serde_json::Value>,
    date: Option<String>,
    description: Option<serde_json::Value>,
    mediatype: Option<String>,
    collection: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct ArchiveFileEntry {
    name: String,
    format: Option<String>,
    size: Option<String>,
    source: Option<String>,
    // Rich metadata from ID3/Vorbis tags (Archive.org extracts these)
    title: Option<String>,
    artist: Option<String>,
    album: Option<String>,
    track: Option<String>,
    length: Option<String>,
    genre: Option<String>,
}

/// Get item metadata and file list.
pub async fn get_item(
    State(state): State<Arc<ApiState>>,
    Path(item_id): Path<String>,
) -> Result<Json<ItemResponse>, (StatusCode, String)> {
    let url = format!("https://archive.org/metadata/{}", item_id);

    let response = state
        .http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("Failed to fetch metadata: {}", e)))?;

    if !response.status().is_success() {
        return Err((
            StatusCode::NOT_FOUND,
            format!("Item not found: {}", item_id),
        ));
    }

    let data: ArchiveMetadataResponse = response
        .json()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("Failed to parse metadata: {}", e)))?;

    let title = match data.metadata.title {
        Some(serde_json::Value::String(s)) => Some(s),
        Some(serde_json::Value::Array(arr)) => arr
            .first()
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        _ => None,
    };

    let creator = match data.metadata.creator {
        Some(serde_json::Value::String(s)) => Some(s),
        Some(serde_json::Value::Array(arr)) => arr
            .first()
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        _ => None,
    };

    let description = match data.metadata.description {
        Some(serde_json::Value::String(s)) => Some(s),
        Some(serde_json::Value::Array(arr)) => arr
            .first()
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        _ => None,
    };

    let collection = match data.metadata.collection {
        Some(serde_json::Value::String(s)) => Some(vec![s]),
        Some(serde_json::Value::Array(arr)) => Some(
            arr.into_iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
        ),
        _ => None,
    };

    let files: Vec<ItemFile> = data
        .files
        .into_iter()
        .map(|f| ItemFile {
            stream_url: format!(
                "/api/v1/archive/items/{}/stream/{}",
                item_id,
                urlencoding::encode(&f.name)
            ),
            name: f.name,
            format: f.format,
            size: f.size,
            source: f.source,
            // Rich metadata from ID3/Vorbis tags
            title: f.title,
            artist: f.artist,
            album: f.album,
            track: f.track,
            length: f.length,
            genre: f.genre,
        })
        .collect();

    Ok(Json(ItemResponse {
        identifier: data.metadata.identifier,
        metadata: ItemMetadata {
            title,
            creator,
            date: data.metadata.date,
            description,
            mediatype: data.metadata.mediatype,
            collection,
        },
        files,
    }))
}

/// Proxy thumbnail from Archive.org.
pub async fn proxy_thumbnail(
    State(state): State<Arc<ApiState>>,
    Path(item_id): Path<String>,
) -> impl IntoResponse {
    let url = format!("https://archive.org/services/img/{}", item_id);

    match state.http_client.get(&url).send().await {
        Ok(response) => {
            if !response.status().is_success() {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap();
            }

            let content_type = response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("image/jpeg")
                .to_string();

            let stream = response.bytes_stream();

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, content_type)
                .header(header::CACHE_CONTROL, "public, max-age=86400")
                .body(Body::from_stream(stream))
                .unwrap()
        }
        Err(_) => Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(Body::empty())
            .unwrap(),
    }
}

/// Proxy audio/file stream from Archive.org.
pub async fn proxy_stream(
    State(state): State<Arc<ApiState>>,
    Path((item_id, file_path)): Path<(String, String)>,
) -> impl IntoResponse {
    let url = format!(
        "https://archive.org/download/{}/{}",
        item_id,
        urlencoding::decode(&file_path).unwrap_or_default()
    );

    match state.http_client.get(&url).send().await {
        Ok(response) => {
            if !response.status().is_success() {
                return Response::builder()
                    .status(response.status())
                    .body(Body::empty())
                    .unwrap();
            }

            let content_type = response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/octet-stream")
                .to_string();

            let content_length = response
                .headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            let stream = response.bytes_stream();

            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, content_type)
                .header(header::ACCEPT_RANGES, "bytes");

            if let Some(length) = content_length {
                builder = builder.header(header::CONTENT_LENGTH, length);
            }

            builder.body(Body::from_stream(stream)).unwrap()
        }
        Err(_) => Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(Body::empty())
            .unwrap(),
    }
}
