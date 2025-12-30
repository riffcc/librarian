//! Node configuration.

use citadel_ping::Capability;
use citadel_spore::U256;
use std::path::PathBuf;

/// Configuration for a Librarian node.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Directory for data storage.
    pub data_dir: PathBuf,

    /// Capabilities this node advertises.
    pub capabilities: Vec<Capability>,

    /// Optional seed for deterministic node ID (testing).
    pub node_id_seed: Option<u64>,

    /// Auth public key bytes for stable node ID (production).
    /// When set, node_id is derived from the auth keypair so it persists across restarts.
    pub auth_pubkey_bytes: Option<[u8; 32]>,
}

impl NodeConfig {
    /// Create a new config with default capabilities.
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            capabilities: vec![
                Capability::Audit,
                Capability::Transcode,
                Capability::Import,
            ],
            node_id_seed: None,
            auth_pubkey_bytes: None,
        }
    }

    /// Generate or derive the node ID.
    /// Priority: auth_pubkey_bytes > node_id_seed > random
    pub fn node_id(&self) -> U256 {
        if let Some(bytes) = &self.auth_pubkey_bytes {
            // Stable ID from auth keypair - persists across restarts
            U256::from_be_bytes(bytes)
        } else if let Some(seed) = self.node_id_seed {
            // Deterministic ID for testing
            U256::from_u64(seed)
        } else {
            // Generate random ID (not recommended for production)
            let mut bytes = [0u8; 32];
            getrandom::getrandom(&mut bytes).expect("Failed to generate random bytes");
            U256::from_be_bytes(&bytes)
        }
    }

    /// Set capabilities.
    pub fn with_capabilities(mut self, caps: Vec<Capability>) -> Self {
        self.capabilities = caps;
        self
    }

    /// Set node ID seed (for testing).
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.node_id_seed = Some(seed);
        self
    }

    /// Set auth public key for stable node ID.
    pub fn with_auth_pubkey(mut self, bytes: [u8; 32]) -> Self {
        self.auth_pubkey_bytes = Some(bytes);
        self
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        let data_dir = directories::ProjectDirs::from("org", "riff", "librarian")
            .map(|d| d.data_dir().to_path_buf())
            .unwrap_or_else(|| PathBuf::from(".librarian"));

        Self::new(data_dir)
    }
}
