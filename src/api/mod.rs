//! REST API for Librarian daemon.
//!
//! Provides HTTP endpoints for:
//! - Archive.org browsing (proxy with thumbnails/streaming)
//! - Import management
//! - Job operations
//! - Quality ladder auditing
//! - Real-time updates via WebSocket

pub mod handlers;

use std::sync::Arc;

use axum::{
    routing::{get, post, delete},
    Router,
};
use citadel_crdt::ContentId;
use tokio::sync::{mpsc, RwLock};
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::Level;

use crate::node::LibrarianNode;

/// Job notification sent when a job is created or needs execution.
#[derive(Debug, Clone)]
pub struct JobNotification {
    pub job_id: ContentId,
}

/// Shared state for API handlers.
pub struct ApiState {
    /// The embedded Citadel node.
    pub node: Arc<RwLock<LibrarianNode>>,

    /// Archivist base URL.
    pub archivist_url: String,

    /// HTTP client for proxying requests.
    pub http_client: reqwest::Client,

    /// Channel to notify worker of new jobs (event-driven, no polling).
    pub job_tx: mpsc::Sender<JobNotification>,
}

impl ApiState {
    /// Create new API state with job notification channel.
    pub fn new(node: LibrarianNode, archivist_url: String, job_tx: mpsc::Sender<JobNotification>) -> Self {
        Self {
            node: Arc::new(RwLock::new(node)),
            archivist_url,
            http_client: reqwest::Client::new(),
            job_tx,
        }
    }

    /// Notify worker that a job is ready for execution.
    pub async fn notify_job(&self, job_id: ContentId) {
        if let Err(e) = self.job_tx.send(JobNotification { job_id }).await {
            tracing::error!("Failed to notify worker of job: {}", e);
        }
    }
}

/// Build the API router with all routes.
pub fn router(state: Arc<ApiState>) -> Router {
    // CORS configuration - allow requests from any origin for development
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        // Status/health
        .route("/api/v1/status", get(handlers::status::health))
        // Archive.org browsing
        .route(
            "/api/v1/archive/search",
            get(handlers::archive::search_items),
        )
        .route(
            "/api/v1/archive/collections/:id",
            get(handlers::archive::get_collection),
        )
        .route(
            "/api/v1/archive/collections/:id/all",
            get(handlers::archive::get_collection_all),
        )
        .route(
            "/api/v1/archive/items/:id",
            get(handlers::archive::get_item),
        )
        .route(
            "/api/v1/archive/items/:id/thumbnail",
            get(handlers::archive::proxy_thumbnail),
        )
        .route(
            "/api/v1/archive/items/:id/stream/*path",
            get(handlers::archive::proxy_stream),
        )
        // Jobs
        .route(
            "/api/v1/jobs",
            get(handlers::jobs::list_jobs).post(handlers::jobs::create_job),
        )
        // Note: /archived must come before /:id to avoid matching "archived" as an ID
        .route("/api/v1/jobs/archived", delete(handlers::jobs::clear_archived_jobs))
        .route("/api/v1/jobs/:id", get(handlers::jobs::get_job).delete(handlers::jobs::delete_job))
        .route("/api/v1/jobs/:id/start", post(handlers::jobs::start_job))
        .route("/api/v1/jobs/:id/stop", post(handlers::jobs::stop_job))
        .route("/api/v1/jobs/:id/retry", post(handlers::jobs::retry_job))
        .route("/api/v1/jobs/:id/archive", post(handlers::jobs::archive_job))
        .route("/api/v1/jobs/:id/metadata", post(handlers::jobs::provide_metadata))
        // Imports (stub for now)
        .route(
            "/api/v1/imports",
            get(handlers::imports::list_imports).post(handlers::imports::create_import),
        )
        .route(
            "/api/v1/imports/:id",
            get(handlers::imports::get_import),
        )
        // WebSocket
        .route("/api/v1/ws", get(handlers::websocket::handler))
        // Sources (URL/CID import)
        .route(
            "/api/v1/sources/import",
            post(handlers::sources::import_source),
        )
        // Middleware
        .layer(cors)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &axum::http::Request<_>| {
                    tracing::info_span!(
                        "request",
                        method = %request.method(),
                        uri = %request.uri(),
                    )
                })
                // Only log requests/responses that are NOT 200 OK
                .on_request(())
                .on_response(|response: &axum::http::Response<_>, latency: std::time::Duration, _span: &tracing::Span| {
                    let status = response.status();
                    if !status.is_success() {
                        tracing::warn!(
                            status = %status,
                            latency_ms = latency.as_millis(),
                            "request failed"
                        );
                    }
                })
        )
        .with_state(state)
}

/// Start the API server.
pub async fn serve(state: Arc<ApiState>, bind_addr: &str) -> anyhow::Result<()> {
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;

    tracing::info!("Librarian API listening on {}", bind_addr);

    axum::serve(listener, app).await?;

    Ok(())
}
