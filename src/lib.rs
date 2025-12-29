//! Librarian - Content operations for Riff.
//!
//! Librarian is the first production user of the Citadel protocol suite.
//! It manages content operations across geodistributed instances using
//! Bilateral CRDTs for zero-latency coordination.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        ARCHIVIST NODES                          │
//! │  Stores actual content: FLAC, Opus, images, manifests           │
//! └───────────────────────────────┬─────────────────────────────────┘
//!                                 │ "what's where"
//! ┌───────────────────────────────┴─────────────────────────────────┐
//! │                        LIBRARIAN                                 │
//! │  Manages: which Archivist node has which content                │
//! │  Operations: audit, migrate, transcode, import                  │
//! │  Uses: Citadel crates for coordination between instances        │
//! └───────────────────────────────┬─────────────────────────────────┘
//!                                 │ uses as crate
//! ┌───────────────────────────────┴─────────────────────────────────┐
//! │                    CITADEL (crates)                              │
//! │  citadel-spore   - O(k) sync layer (foundation)                 │
//! │  citadel-crdt    - Bilateral CRDT traits                        │
//! │  citadel-docs    - Document storage                             │
//! │  citadel-gossip  - Ephemeral messages                           │
//! │  citadel-ping    - Health/presence                              │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Key Properties (Proven in Lean)
//!
//! - **Zero latency writes**: Apply locally, no network wait
//! - **Partition tolerant**: Works offline, during network failures
//! - **Always available**: Every operation succeeds
//! - **Proven convergent**: Same operations → same state

// === Core Modules ===

/// CRDT types for Librarian.
pub mod crdt;

/// Embedded Citadel node.
pub mod node;

/// REST API.
pub mod api;

/// Background job worker.
pub mod worker;

/// Authentication for Archivist uploads.
pub mod auth;

// === External Service Clients ===

/// Archive.org API client (preserved from original).
pub mod archive_api;

// === TUI Modules (to be migrated) ===

/// Application state.
pub mod app;

/// Event handling.
pub mod event;

/// Settings management.
pub mod settings;

/// Terminal UI.
pub mod tui;

/// UI rendering.
pub mod ui;

/// Input handling and state updates.
pub mod update;

// === Re-exports ===

pub use crdt::{Job, JobOp, JobResult, JobStatus, JobTarget, JobType};
pub use node::{LibrarianNode, NodeConfig, NodeError};
