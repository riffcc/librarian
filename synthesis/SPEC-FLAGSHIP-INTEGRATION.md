# Librarian: Flagship Integration Specification

## Specification Document
**Version:** 1.0.0
**Date:** 2025-12-25
**Status:** Draft

---

## Executive Summary

This specification defines the integration between Librarian and Flagship, transforming Librarian from a standalone TUI archival tool into a content acquisition backend that can push imported content to Flagship for moderation and publication.

---

## Part 1: Architecture Evolution

### 1.1 Current State

```
┌─────────────────────────────────────────────┐
│                 LIBRARIAN                    │
│  ┌─────────────────────────────────────┐    │
│  │           TUI Interface             │    │
│  │  • Browse collections               │    │
│  │  • View items                       │    │
│  │  • Download to local disk           │    │
│  └─────────────────────────────────────┘    │
│                    │                         │
│  ┌─────────────────┴─────────────────┐      │
│  │         Archive.org API           │      │
│  │  • Fetch collection items         │      │
│  │  • Get item metadata              │      │
│  │  • Download files                 │      │
│  └───────────────────────────────────┘      │
│                    │                         │
│                    ▼                         │
│  ┌───────────────────────────────────┐      │
│  │         Local Filesystem          │      │
│  │  $download_dir/                   │      │
│  │  ├── .item_cache/                 │      │
│  │  │   └── {collection}.json        │      │
│  │  └── {item_id}/                   │      │
│  │      └── {files}                  │      │
│  └───────────────────────────────────┘      │
└─────────────────────────────────────────────┘
```

### 1.2 Target State

```
┌─────────────────────────────────────────────────────────────────────┐
│                            LIBRARIAN                                 │
│                                                                      │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │
│  │   TUI Interface  │  │    API Server    │  │   CLI Commands   │  │
│  │   (interactive)  │  │   (headless)     │  │   (scripting)    │  │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘  │
│           │                     │                     │             │
│  ┌────────┴─────────────────────┴─────────────────────┴────────┐   │
│  │                      Core Engine                             │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │   │
│  │  │ Archive.org │  │    HTTP     │  │     S3      │          │   │
│  │  │   Source    │  │   Source    │  │   Source    │          │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘          │   │
│  │                                                              │   │
│  │  ┌─────────────────────────────────────────────────────┐    │   │
│  │  │              Import Coordinator                      │    │   │
│  │  │  • Job queue management                              │    │   │
│  │  │  • Progress tracking                                 │    │   │
│  │  │  • Rate limiting                                     │    │   │
│  │  │  • Error handling & retries                          │    │   │
│  │  └─────────────────────────────────────────────────────┘    │   │
│  │                                                              │   │
│  │  ┌─────────────────────────────────────────────────────┐    │   │
│  │  │              Output Destinations                     │    │   │
│  │  │  ┌─────────────┐  ┌─────────────┐  ┌────────────┐   │    │   │
│  │  │  │Local Files  │  │  Flagship   │  │  Archivist │   │    │   │
│  │  │  │  (current)  │  │    API      │  │    API     │   │    │   │
│  │  │  └─────────────┘  └─────────────┘  └────────────┘   │    │   │
│  │  └─────────────────────────────────────────────────────┘    │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                │ Push to Flagship
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                           FLAGSHIP                                   │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    Import API                                │   │
│  │  POST /api/v1/imports                                       │   │
│  │  POST /api/v1/imports/progress                              │   │
│  │  GET  /api/v1/imports/:id/status                            │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                │                                     │
│                                ▼                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              Pending Content Queue                           │   │
│  │  • Greyed tiles visible to admins                           │   │
│  │  • Preview capability                                        │   │
│  │  • Approve/Reject workflow                                   │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Part 2: Core Refactoring

### 2.1 Module Structure

```
src/
├── main.rs                    # Entry point - selects mode (TUI/API/CLI)
├── lib.rs                     # Library exports
│
├── core/                      # Core business logic
│   ├── mod.rs
│   ├── engine.rs              # Import engine (coordinates everything)
│   ├── job.rs                 # Job definition and state
│   ├── queue.rs               # Job queue management
│   └── progress.rs            # Progress tracking
│
├── sources/                   # Content sources
│   ├── mod.rs
│   ├── traits.rs              # ContentSource trait
│   ├── archive_org.rs         # Archive.org source (existing, refactored)
│   ├── http.rs                # HTTP/HTTPS URL source
│   └── s3.rs                  # S3 bucket source
│
├── destinations/              # Output destinations
│   ├── mod.rs
│   ├── traits.rs              # ContentDestination trait
│   ├── local.rs               # Local filesystem (existing)
│   ├── flagship.rs            # Flagship API destination
│   └── archivist.rs           # Direct Archivist upload
│
├── interfaces/                # User interfaces
│   ├── mod.rs
│   ├── tui/                   # Terminal UI (existing, refactored)
│   │   ├── mod.rs
│   │   ├── app.rs
│   │   ├── ui.rs
│   │   └── event.rs
│   ├── api/                   # HTTP API server
│   │   ├── mod.rs
│   │   ├── server.rs
│   │   └── routes.rs
│   └── cli/                   # CLI commands
│       ├── mod.rs
│       └── commands.rs
│
├── config/                    # Configuration
│   ├── mod.rs
│   └── settings.rs            # Settings (existing, extended)
│
└── utils/                     # Utilities
    ├── mod.rs
    ├── rate_limiter.rs        # Rate limiting (existing)
    └── metadata.rs            # Metadata extraction
```

### 2.2 Core Traits

```rust
// src/sources/traits.rs

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Metadata for an item from any source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemMetadata {
    pub external_id: String,
    pub title: Option<String>,
    pub creator: Option<String>,
    pub description: Option<String>,
    pub date: Option<String>,
    pub media_type: Option<String>,
    pub collections: Vec<String>,
    pub extra: serde_json::Value,
}

/// A file within an item
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemFile {
    pub name: String,
    pub size: Option<u64>,
    pub format: Option<String>,
    pub md5: Option<String>,
    pub download_url: String,
}

/// Complete item with metadata and files
#[derive(Debug, Clone)]
pub struct Item {
    pub metadata: ItemMetadata,
    pub files: Vec<ItemFile>,
}

/// Progress callback for downloads
pub type ProgressCallback = Box<dyn Fn(ProgressEvent) + Send + Sync>;

#[derive(Debug, Clone)]
pub enum ProgressEvent {
    ItemStarted { id: String, name: String },
    ItemFileCount { count: usize },
    FileStarted { name: String, size: Option<u64> },
    BytesDownloaded { bytes: u64 },
    FileCompleted { name: String },
    ItemCompleted { id: String, success: bool },
    Error { message: String },
}

/// Trait for content sources (Archive.org, HTTP, S3, etc.)
#[async_trait]
pub trait ContentSource: Send + Sync {
    /// Get the source type identifier
    fn source_type(&self) -> &'static str;

    /// List items available from this source
    async fn list_items(&self, filter: &SourceFilter) -> Result<Vec<ItemMetadata>>;

    /// Get full details for a specific item
    async fn get_item(&self, id: &str) -> Result<Item>;

    /// Download a file from this source
    async fn download_file(
        &self,
        file: &ItemFile,
        destination: &Path,
        on_progress: Option<ProgressCallback>,
    ) -> Result<()>;
}

#[derive(Debug, Clone, Default)]
pub struct SourceFilter {
    pub collection: Option<String>,
    pub media_type: Option<String>,
    pub date_range: Option<(String, String)>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}
```

```rust
// src/destinations/traits.rs

use async_trait::async_trait;
use crate::sources::traits::{Item, ItemFile, ProgressCallback};

/// Result of pushing content to a destination
#[derive(Debug, Clone)]
pub struct PushResult {
    pub success: bool,
    pub destination_id: Option<String>,  // e.g., Flagship release ID
    pub message: Option<String>,
}

/// Trait for content destinations (local, Flagship, Archivist)
#[async_trait]
pub trait ContentDestination: Send + Sync {
    /// Get the destination type identifier
    fn destination_type(&self) -> &'static str;

    /// Push an item to this destination
    async fn push_item(
        &self,
        item: &Item,
        files: &[(&ItemFile, &Path)],  // File metadata + local path
        options: &PushOptions,
        on_progress: Option<ProgressCallback>,
    ) -> Result<PushResult>;

    /// Check if destination is available
    async fn health_check(&self) -> Result<bool>;
}

#[derive(Debug, Clone, Default)]
pub struct PushOptions {
    pub auto_approve: bool,
    pub category: Option<String>,
    pub visibility: Option<String>,
    pub assignee: Option<String>,
}
```

### 2.3 Import Engine

```rust
// src/core/engine.rs

use std::sync::Arc;
use tokio::sync::mpsc;

pub struct ImportEngine {
    sources: HashMap<String, Arc<dyn ContentSource>>,
    destinations: HashMap<String, Arc<dyn ContentDestination>>,
    job_queue: JobQueue,
    rate_limiter: AppRateLimiter,
    config: EngineConfig,
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub max_concurrent_downloads: usize,
    pub max_concurrent_uploads: usize,
    pub retry_attempts: u32,
    pub retry_delay_ms: u64,
    pub temp_dir: PathBuf,
}

impl ImportEngine {
    pub fn new(config: EngineConfig, rate_limiter: AppRateLimiter) -> Self {
        Self {
            sources: HashMap::new(),
            destinations: HashMap::new(),
            job_queue: JobQueue::new(),
            rate_limiter,
            config,
        }
    }

    pub fn register_source(&mut self, name: &str, source: Arc<dyn ContentSource>) {
        self.sources.insert(name.to_string(), source);
    }

    pub fn register_destination(&mut self, name: &str, dest: Arc<dyn ContentDestination>) {
        self.destinations.insert(name.to_string(), dest);
    }

    /// Create a new import job
    pub async fn create_job(&self, request: ImportRequest) -> Result<JobId> {
        let job = Job::new(request);
        let job_id = job.id.clone();
        self.job_queue.enqueue(job).await?;
        Ok(job_id)
    }

    /// Start processing jobs
    pub async fn run(&self, progress_tx: mpsc::Sender<JobProgress>) -> Result<()> {
        loop {
            if let Some(job) = self.job_queue.dequeue().await {
                let progress = self.process_job(job).await;
                progress_tx.send(progress).await?;
            }
        }
    }

    async fn process_job(&self, job: Job) -> JobProgress {
        let source = self.sources.get(&job.request.source_type)
            .ok_or_else(|| anyhow!("Unknown source: {}", job.request.source_type))?;

        let destination = self.destinations.get(&job.request.destination_type)
            .ok_or_else(|| anyhow!("Unknown destination: {}", job.request.destination_type))?;

        // 1. List items from source
        let items = source.list_items(&job.request.filter).await?;

        // 2. Process each item
        for item_meta in items {
            // Get full item details
            let item = source.get_item(&item_meta.external_id).await?;

            // Download files to temp directory
            let temp_dir = self.config.temp_dir.join(&item_meta.external_id);
            fs::create_dir_all(&temp_dir)?;

            let mut downloaded_files = Vec::new();
            for file in &item.files {
                let file_path = temp_dir.join(&file.name);
                source.download_file(file, &file_path, None).await?;
                downloaded_files.push((file, file_path));
            }

            // Push to destination
            let file_refs: Vec<_> = downloaded_files.iter()
                .map(|(f, p)| (*f, p.as_path()))
                .collect();

            destination.push_item(
                &item,
                &file_refs,
                &job.request.push_options,
                None,
            ).await?;

            // Cleanup temp files
            fs::remove_dir_all(&temp_dir)?;
        }

        JobProgress {
            job_id: job.id,
            status: JobStatus::Completed,
            items_processed: items.len(),
            errors: vec![],
        }
    }
}
```

---

## Part 3: Flagship Destination Implementation

### 3.1 Flagship API Client

```rust
// src/destinations/flagship.rs

use reqwest::Client;
use serde::{Deserialize, Serialize};

pub struct FlagshipDestination {
    client: Client,
    base_url: String,
    api_key: String,
    archivist_url: Option<String>,
}

#[derive(Debug, Serialize)]
struct CreateImportRequest {
    source: ImportSourceInfo,
    items: Vec<ImportItem>,
    options: ImportOptions,
}

#[derive(Debug, Serialize)]
struct ImportSourceInfo {
    #[serde(rename = "type")]
    source_type: String,
    collection_id: Option<String>,
    base_url: Option<String>,
}

#[derive(Debug, Serialize)]
struct ImportItem {
    external_id: String,
    suggested_name: String,
    suggested_category: String,
    metadata: serde_json::Value,
    files: Vec<ImportFile>,
    thumbnail_url: Option<String>,
}

#[derive(Debug, Serialize)]
struct ImportFile {
    name: String,
    cid: String,  // Already uploaded to Archivist
    size: u64,
    format: Option<String>,
}

#[derive(Debug, Serialize)]
struct ImportOptions {
    auto_approve: bool,
    assignee: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ImportResponse {
    import_id: String,
    status: String,
    created_releases: Vec<CreatedRelease>,
}

#[derive(Debug, Deserialize)]
struct CreatedRelease {
    id: String,
    external_id: String,
    name: String,
    status: String,
}

#[async_trait]
impl ContentDestination for FlagshipDestination {
    fn destination_type(&self) -> &'static str {
        "flagship"
    }

    async fn push_item(
        &self,
        item: &Item,
        files: &[(&ItemFile, &Path)],
        options: &PushOptions,
        on_progress: Option<ProgressCallback>,
    ) -> Result<PushResult> {
        // 1. Upload files to Archivist first (if configured)
        let mut uploaded_files = Vec::new();
        if let Some(archivist_url) = &self.archivist_url {
            for (file_meta, file_path) in files {
                let cid = self.upload_to_archivist(archivist_url, file_path).await?;
                uploaded_files.push(ImportFile {
                    name: file_meta.name.clone(),
                    cid,
                    size: file_meta.size.unwrap_or(0),
                    format: file_meta.format.clone(),
                });

                if let Some(ref callback) = on_progress {
                    callback(ProgressEvent::FileCompleted {
                        name: file_meta.name.clone(),
                    });
                }
            }
        }

        // 2. Create import in Flagship
        let request = CreateImportRequest {
            source: ImportSourceInfo {
                source_type: "librarian".to_string(),
                collection_id: item.metadata.collections.first().cloned(),
                base_url: None,
            },
            items: vec![ImportItem {
                external_id: item.metadata.external_id.clone(),
                suggested_name: item.metadata.title.clone()
                    .unwrap_or_else(|| item.metadata.external_id.clone()),
                suggested_category: self.infer_category(&item.metadata),
                metadata: serde_json::to_value(&item.metadata)?,
                files: uploaded_files,
                thumbnail_url: self.find_thumbnail(files),
            }],
            options: ImportOptions {
                auto_approve: options.auto_approve,
                assignee: options.assignee.clone(),
            },
        };

        let response: ImportResponse = self.client
            .post(&format!("{}/api/v1/imports", self.base_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&request)
            .send()
            .await?
            .json()
            .await?;

        Ok(PushResult {
            success: true,
            destination_id: response.created_releases.first()
                .map(|r| r.id.clone()),
            message: Some(format!("Created import {}", response.import_id)),
        })
    }

    async fn health_check(&self) -> Result<bool> {
        let response = self.client
            .get(&format!("{}/api/v1/health", self.base_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .send()
            .await?;

        Ok(response.status().is_success())
    }
}

impl FlagshipDestination {
    pub fn new(config: FlagshipConfig) -> Self {
        Self {
            client: Client::new(),
            base_url: config.base_url,
            api_key: config.api_key,
            archivist_url: config.archivist_url,
        }
    }

    async fn upload_to_archivist(&self, archivist_url: &str, file_path: &Path) -> Result<String> {
        let file = tokio::fs::File::open(file_path).await?;
        let stream = tokio_util::io::ReaderStream::new(file);
        let body = reqwest::Body::wrap_stream(stream);

        let response: serde_json::Value = self.client
            .post(&format!("{}/api/v1/upload", archivist_url))
            .body(body)
            .send()
            .await?
            .json()
            .await?;

        response["cid"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("No CID in archivist response"))
    }

    fn infer_category(&self, metadata: &ItemMetadata) -> String {
        match metadata.media_type.as_deref() {
            Some("audio") | Some("etree") => "music",
            Some("movies") | Some("feature_films") => "movies",
            Some("texts") | Some("books") => "books",
            Some("software") | Some("games") => "software",
            _ => "other",
        }.to_string()
    }

    fn find_thumbnail(&self, files: &[(&ItemFile, &Path)]) -> Option<String> {
        // Look for common thumbnail filenames
        for (file_meta, _) in files {
            let name_lower = file_meta.name.to_lowercase();
            if name_lower.ends_with(".jpg") || name_lower.ends_with(".png") {
                if name_lower.contains("cover")
                    || name_lower.contains("thumb")
                    || name_lower.contains("front")
                {
                    return Some(file_meta.download_url.clone());
                }
            }
        }
        None
    }
}
```

### 3.2 Configuration Extension

```rust
// src/config/settings.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    // Existing settings
    pub download_directory: Option<String>,
    pub favorite_collections: Vec<String>,

    // New: Flagship integration
    pub flagship: Option<FlagshipConfig>,

    // New: Archivist integration
    pub archivist: Option<ArchivistConfig>,

    // New: Default destination
    pub default_destination: String,  // "local", "flagship", "archivist"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlagshipConfig {
    pub base_url: String,
    pub api_key: String,
    pub archivist_url: Option<String>,  // If set, upload files before creating import
    pub auto_approve: bool,
    pub default_category: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchivistConfig {
    pub base_url: String,
    pub api_key: Option<String>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            download_directory: None,
            favorite_collections: vec![
                "enough_records".to_string(),
                "netlabels".to_string(),
            ],
            flagship: None,
            archivist: None,
            default_destination: "local".to_string(),
        }
    }
}
```

---

## Part 4: API Server

### 4.1 HTTP Server

```rust
// src/interfaces/api/server.rs

use axum::{
    Router,
    routing::{get, post},
    extract::{State, Json},
    response::IntoResponse,
};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct ApiServer {
    engine: Arc<RwLock<ImportEngine>>,
    config: ApiServerConfig,
}

#[derive(Debug, Clone)]
pub struct ApiServerConfig {
    pub host: String,
    pub port: u16,
    pub api_key: Option<String>,  // For authentication
}

impl ApiServer {
    pub fn new(engine: ImportEngine, config: ApiServerConfig) -> Self {
        Self {
            engine: Arc::new(RwLock::new(engine)),
            config,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let app = Router::new()
            .route("/health", get(health_check))
            .route("/api/v1/jobs", post(create_job))
            .route("/api/v1/jobs/:id", get(get_job_status))
            .route("/api/v1/jobs/:id/cancel", post(cancel_job))
            .route("/api/v1/sources", get(list_sources))
            .route("/api/v1/sources/:source/collections", get(list_collections))
            .route("/api/v1/sources/:source/items", get(list_items))
            .with_state(self.engine.clone());

        let addr = format!("{}:{}", self.config.host, self.config.port);
        axum::Server::bind(&addr.parse()?)
            .serve(app.into_make_service())
            .await?;

        Ok(())
    }
}

// Route handlers

async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

async fn create_job(
    State(engine): State<Arc<RwLock<ImportEngine>>>,
    Json(request): Json<CreateJobRequest>,
) -> impl IntoResponse {
    let engine = engine.read().await;
    match engine.create_job(request.into()).await {
        Ok(job_id) => Json(serde_json::json!({
            "job_id": job_id,
            "status": "queued",
        })),
        Err(e) => Json(serde_json::json!({
            "error": e.to_string(),
        })),
    }
}

#[derive(Debug, Deserialize)]
struct CreateJobRequest {
    source: String,  // "archive-org", "http", "s3"
    destination: String,  // "local", "flagship", "archivist"
    filter: SourceFilter,
    options: PushOptions,
}
```

### 4.2 CLI Commands

```rust
// src/interfaces/cli/commands.rs

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "librarian")]
#[command(about = "Content curation, scraping, and Flagship integration")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Configuration file path
    #[arg(short, long, default_value = "~/.config/librarian/config.toml")]
    pub config: String,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the TUI interface
    Tui,

    /// Start the API server
    Serve {
        #[arg(short, long, default_value = "127.0.0.1")]
        host: String,
        #[arg(short, long, default_value = "8000")]
        port: u16,
    },

    /// Import content from a source
    Import {
        /// Source type (archive-org, http, s3)
        #[arg(short, long)]
        source: String,

        /// Destination (local, flagship, archivist)
        #[arg(short, long, default_value = "local")]
        destination: String,

        /// Collection or URL to import from
        #[arg(short, long)]
        collection: Option<String>,

        /// Maximum items to import
        #[arg(long)]
        limit: Option<usize>,

        /// Auto-approve items (for Flagship destination)
        #[arg(long)]
        auto_approve: bool,
    },

    /// List available collections from a source
    List {
        #[arg(short, long, default_value = "archive-org")]
        source: String,
    },

    /// Configure Flagship integration
    ConfigureFlagship {
        #[arg(long)]
        url: String,
        #[arg(long)]
        api_key: String,
        #[arg(long)]
        archivist_url: Option<String>,
    },
}

pub async fn run_cli(cli: Cli) -> Result<()> {
    let config = load_config(&cli.config)?;

    match cli.command {
        Commands::Tui => {
            run_tui(config).await
        }
        Commands::Serve { host, port } => {
            run_api_server(config, host, port).await
        }
        Commands::Import { source, destination, collection, limit, auto_approve } => {
            run_import(config, source, destination, collection, limit, auto_approve).await
        }
        Commands::List { source } => {
            list_collections(config, source).await
        }
        Commands::ConfigureFlagship { url, api_key, archivist_url } => {
            configure_flagship(config, url, api_key, archivist_url).await
        }
    }
}

async fn run_import(
    config: Settings,
    source: String,
    destination: String,
    collection: Option<String>,
    limit: Option<usize>,
    auto_approve: bool,
) -> Result<()> {
    let engine = create_engine(&config)?;

    let request = ImportRequest {
        source_type: source,
        destination_type: destination,
        filter: SourceFilter {
            collection,
            limit,
            ..Default::default()
        },
        push_options: PushOptions {
            auto_approve,
            ..Default::default()
        },
    };

    let job_id = engine.create_job(request).await?;
    println!("Created job: {}", job_id);

    // Wait for completion with progress output
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    tokio::spawn(async move {
        engine.run(tx).await
    });

    while let Some(progress) = rx.recv().await {
        println!("{:?}", progress);
        if matches!(progress.status, JobStatus::Completed | JobStatus::Failed) {
            break;
        }
    }

    Ok(())
}
```

---

## Part 5: HTTP Source Implementation

### 5.1 HTTP/HTTPS Importer

```rust
// src/sources/http.rs

use scraper::{Html, Selector};

pub struct HttpSource {
    client: Client,
    rate_limiter: AppRateLimiter,
}

#[async_trait]
impl ContentSource for HttpSource {
    fn source_type(&self) -> &'static str {
        "http"
    }

    async fn list_items(&self, filter: &SourceFilter) -> Result<Vec<ItemMetadata>> {
        let url = filter.collection.as_ref()
            .ok_or_else(|| anyhow!("HTTP source requires a URL in collection field"))?;

        self.rate_limiter.until_ready().await;
        let response = self.client.get(url).send().await?;
        let content_type = response.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if content_type.contains("text/html") {
            // Parse HTML page for media links
            let html = response.text().await?;
            self.extract_media_from_html(url, &html).await
        } else {
            // Direct file URL
            let filename = url.split('/').last()
                .unwrap_or("unknown");
            Ok(vec![ItemMetadata {
                external_id: url.clone(),
                title: Some(filename.to_string()),
                ..Default::default()
            }])
        }
    }

    async fn get_item(&self, id: &str) -> Result<Item> {
        // For HTTP source, the ID is the URL
        let metadata = ItemMetadata {
            external_id: id.to_string(),
            title: id.split('/').last().map(String::from),
            ..Default::default()
        };

        let files = vec![ItemFile {
            name: id.split('/').last().unwrap_or("file").to_string(),
            download_url: id.to_string(),
            size: None,
            format: None,
            md5: None,
        }];

        Ok(Item { metadata, files })
    }

    async fn download_file(
        &self,
        file: &ItemFile,
        destination: &Path,
        on_progress: Option<ProgressCallback>,
    ) -> Result<()> {
        self.rate_limiter.until_ready().await;

        let response = self.client.get(&file.download_url).send().await?;
        let total_size = response.content_length();

        let mut dest_file = tokio::fs::File::create(destination).await?;
        let mut stream = response.bytes_stream();
        let mut downloaded = 0u64;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            dest_file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;

            if let Some(ref callback) = on_progress {
                callback(ProgressEvent::BytesDownloaded { bytes: chunk.len() as u64 });
            }
        }

        Ok(())
    }
}

impl HttpSource {
    async fn extract_media_from_html(&self, base_url: &str, html: &str) -> Result<Vec<ItemMetadata>> {
        let document = Html::parse_document(html);
        let link_selector = Selector::parse("a[href]").unwrap();

        let mut items = Vec::new();
        let audio_extensions = [".mp3", ".flac", ".m4a", ".ogg", ".wav"];
        let video_extensions = [".mp4", ".mkv", ".webm", ".avi"];

        for element in document.select(&link_selector) {
            if let Some(href) = element.value().attr("href") {
                let full_url = if href.starts_with("http") {
                    href.to_string()
                } else {
                    format!("{}/{}", base_url.trim_end_matches('/'), href.trim_start_matches('/'))
                };

                let is_media = audio_extensions.iter().any(|ext| href.ends_with(ext))
                    || video_extensions.iter().any(|ext| href.ends_with(ext));

                if is_media {
                    items.push(ItemMetadata {
                        external_id: full_url.clone(),
                        title: href.split('/').last().map(String::from),
                        ..Default::default()
                    });
                }
            }
        }

        Ok(items)
    }
}
```

---

## Part 6: S3 Source Implementation

### 5.1 S3 Bucket Importer

```rust
// src/sources/s3.rs

use aws_sdk_s3::Client as S3Client;

pub struct S3Source {
    client: S3Client,
    config: S3SourceConfig,
}

#[derive(Debug, Clone)]
pub struct S3SourceConfig {
    pub bucket: String,
    pub region: String,
    pub prefix: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[async_trait]
impl ContentSource for S3Source {
    fn source_type(&self) -> &'static str {
        "s3"
    }

    async fn list_items(&self, filter: &SourceFilter) -> Result<Vec<ItemMetadata>> {
        let prefix = filter.collection.clone()
            .or_else(|| self.config.prefix.clone())
            .unwrap_or_default();

        let mut items = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut request = self.client.list_objects_v2()
                .bucket(&self.config.bucket)
                .prefix(&prefix);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await?;

            for object in response.contents.unwrap_or_default() {
                if let Some(key) = object.key {
                    // Group by folder (first path segment after prefix)
                    let relative_key = key.strip_prefix(&prefix)
                        .unwrap_or(&key);

                    items.push(ItemMetadata {
                        external_id: key.clone(),
                        title: Some(relative_key.to_string()),
                        ..Default::default()
                    });
                }
            }

            if response.is_truncated == Some(true) {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }

        Ok(items)
    }

    async fn get_item(&self, id: &str) -> Result<Item> {
        let metadata = ItemMetadata {
            external_id: id.to_string(),
            title: id.split('/').last().map(String::from),
            ..Default::default()
        };

        // Get object metadata
        let head = self.client.head_object()
            .bucket(&self.config.bucket)
            .key(id)
            .send()
            .await?;

        let files = vec![ItemFile {
            name: id.split('/').last().unwrap_or("file").to_string(),
            download_url: format!("s3://{}/{}", self.config.bucket, id),
            size: head.content_length.map(|l| l as u64),
            format: head.content_type,
            md5: head.e_tag.map(|t| t.trim_matches('"').to_string()),
        }];

        Ok(Item { metadata, files })
    }

    async fn download_file(
        &self,
        file: &ItemFile,
        destination: &Path,
        on_progress: Option<ProgressCallback>,
    ) -> Result<()> {
        // Extract key from s3:// URL
        let key = file.download_url
            .strip_prefix(&format!("s3://{}/", self.config.bucket))
            .ok_or_else(|| anyhow!("Invalid S3 URL"))?;

        let response = self.client.get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await?;

        let mut dest_file = tokio::fs::File::create(destination).await?;
        let mut stream = response.body;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            dest_file.write_all(&chunk).await?;

            if let Some(ref callback) = on_progress {
                callback(ProgressEvent::BytesDownloaded { bytes: chunk.len() as u64 });
            }
        }

        Ok(())
    }
}
```

---

## Part 7: TUI Enhancements

### 7.1 Destination Selection

```rust
// Add to TUI app state
pub struct App {
    // ... existing fields ...

    /// Current destination mode
    pub destination_mode: DestinationMode,

    /// Flagship connection status
    pub flagship_status: Option<ConnectionStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DestinationMode {
    Local,
    Flagship,
    Archivist,
}

#[derive(Debug, Clone)]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
    Checking,
    Error(String),
}
```

### 7.2 New TUI Views

```
┌─────────────────────────────────────────────────────────────────────┐
│  Librarian                                              [D] [↑] [↓] │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─ Collections ──────┐ ┌─ Items ─────────────────────────────────┐ │
│  │                    │ │                                         │ │
│  │  ▶ enough_records  │ │  ▶ litmus_perception_of_light         │ │
│  │    netlabels       │ │    various_compilation_vol_2          │ │
│  │    blocksonic      │ │    artist_album_name                  │ │
│  │    ccmixter        │ │    ...                                │ │
│  │                    │ │                                         │ │
│  │  [A]dd [R]emove    │ │  Total: 1,234 items                    │ │
│  └────────────────────┘ └─────────────────────────────────────────┘ │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│  Destination: [F]lagship ● Connected                                │
│  Press [D] to download · [I] to import to Flagship · [S] settings  │
└─────────────────────────────────────────────────────────────────────┘

Press 'I' to import to Flagship:

┌─────────────────────────────────────────────────────────────────────┐
│  Import to Flagship                                      [X] Cancel │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Source: enough_records (1,234 items)                               │
│                                                                     │
│  Options:                                                           │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  [ ] Auto-approve (skip moderation)                         │   │
│  │  Category: [Music            ▼]                             │   │
│  │  Limit:    [100              ]  (0 = all)                   │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  This will:                                                         │
│  • Download files from Archive.org                                  │
│  • Upload to Archivist (if configured)                              │
│  • Create pending releases in Flagship                              │
│                                                                     │
│                               [Cancel]  [Start Import]              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Part 8: Implementation Phases

### Phase 1: Core Refactoring (1 week)
- [ ] Extract core engine from TUI
- [ ] Define ContentSource and ContentDestination traits
- [ ] Refactor Archive.org source to use new traits
- [ ] Refactor local filesystem destination

### Phase 2: Flagship Integration (1 week)
- [ ] Implement FlagshipDestination
- [ ] Add Archivist upload support
- [ ] Create configuration for Flagship connection
- [ ] Test end-to-end import flow

### Phase 3: API Server (1 week)
- [ ] Implement Axum HTTP server
- [ ] Create job management endpoints
- [ ] Add authentication
- [ ] Implement progress webhooks

### Phase 4: CLI Commands (3 days)
- [ ] Implement clap command structure
- [ ] Add import command
- [ ] Add configuration commands
- [ ] Add list/browse commands

### Phase 5: Additional Sources (1 week)
- [ ] Implement HTTP source
- [ ] Implement S3 source
- [ ] Test with various content types

### Phase 6: TUI Enhancements (3 days)
- [ ] Add destination selection
- [ ] Add connection status display
- [ ] Add import dialog
- [ ] Update keybindings

---

## Appendix A: API Reference

### Create Import Job

```http
POST /api/v1/jobs
Content-Type: application/json
Authorization: Bearer <api_key>

{
  "source": "archive-org",
  "destination": "flagship",
  "filter": {
    "collection": "enough_records",
    "limit": 100
  },
  "options": {
    "auto_approve": false,
    "category": "music"
  }
}

Response:
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "queued",
  "created_at": "2024-12-25T14:30:00Z"
}
```

### Get Job Status

```http
GET /api/v1/jobs/550e8400-e29b-41d4-a716-446655440000
Authorization: Bearer <api_key>

Response:
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "processing",
  "progress": {
    "items_total": 100,
    "items_processed": 45,
    "items_failed": 2,
    "current_item": "litmus_perception_of_light"
  },
  "created_at": "2024-12-25T14:30:00Z",
  "started_at": "2024-12-25T14:30:05Z"
}
```

---

## Appendix B: Configuration File

```toml
# ~/.config/librarian/config.toml

[general]
download_directory = "/data/librarian"
default_destination = "flagship"

[archive_org]
rate_limit_per_minute = 15

[flagship]
base_url = "https://riff.cc"
api_key = "your-api-key-here"
archivist_url = "https://archivist.riff.cc"
auto_approve = false
default_category = "music"

[archivist]
base_url = "https://archivist.riff.cc"

[favorite_collections]
collections = [
  "enough_records",
  "netlabels",
  "blocksonic",
  "ccmixter",
]
```
