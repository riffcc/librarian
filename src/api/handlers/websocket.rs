//! WebSocket handler for real-time updates.

use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;

use crate::api::ApiState;

/// WebSocket message types.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsMessage {
    /// Job progress update.
    JobProgress {
        job_id: String,
        progress: f32,
        message: Option<String>,
    },

    /// Job status changed.
    JobStatusChanged {
        job_id: String,
        status: String,
    },

    /// Import progress update.
    ImportProgress {
        import_id: String,
        status: String,
        progress: f32,
    },

    /// Peer status update.
    PeerUpdate {
        peer_count: usize,
        load: f32,
    },
}

/// WebSocket upgrade handler.
pub async fn handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

/// Handle WebSocket connection.
async fn handle_socket(socket: WebSocket, state: Arc<ApiState>) {
    let (mut sender, mut receiver) = socket.split();

    // Send initial status
    {
        let node = state.node.read().await;
        let status = WsMessage::PeerUpdate {
            peer_count: node.peer_count(),
            load: node.load().await,
        };

        if let Ok(json) = serde_json::to_string(&status) {
            let _ = sender.send(Message::Text(json.into())).await;
        }
    }

    // Handle incoming messages (for future command support)
    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                tracing::debug!("Received WebSocket message: {}", text);
                // TODO: Handle incoming commands
            }
            Ok(Message::Close(_)) => {
                tracing::debug!("WebSocket client disconnected");
                break;
            }
            Err(e) => {
                tracing::warn!("WebSocket error: {}", e);
                break;
            }
            _ => {}
        }
    }
}
