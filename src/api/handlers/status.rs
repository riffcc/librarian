//! Status and health check handlers.

use std::sync::Arc;

use axum::{extract::State, Json};
use serde::Serialize;

use crate::api::ApiState;

/// Health response.
#[derive(Serialize)]
pub struct HealthResponse {
    /// Service status.
    pub status: String,

    /// Node ID (hex-encoded).
    pub node_id: String,

    /// Number of known peers.
    pub peer_count: usize,

    /// Number of pending jobs.
    pub pending_jobs: usize,

    /// Number of running jobs.
    pub running_jobs: usize,

    /// Current node load (0.0 - 1.0).
    pub load: f32,
}

/// Health check endpoint.
pub async fn health(State(state): State<Arc<ApiState>>) -> Json<HealthResponse> {
    let node = state.node.read().await;

    let pending_jobs = node
        .list_jobs_by_status(crate::crdt::JobStatus::Pending)
        .unwrap_or_default()
        .len();

    let running_jobs = node
        .list_jobs_by_status(crate::crdt::JobStatus::Running)
        .unwrap_or_default()
        .len();

    Json(HealthResponse {
        status: "ok".to_string(),
        node_id: hex::encode(node.node_id().to_be_bytes()),
        peer_count: node.peer_count(),
        pending_jobs,
        running_jobs,
        load: node.load().await,
    })
}
