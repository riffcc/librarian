//! Authentication module for Librarian.
//!
//! Manages Ed25519 keypairs for signing upload requests to Archivist.
//! Compatible with lens-admin and Flagship authentication format.

use std::fs;
use std::path::{Path, PathBuf};

use ed25519_dalek::{SigningKey, Signer, SECRET_KEY_LENGTH};
use thiserror::Error;

/// Auth errors.
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("No keypair found at {0}. Run 'librarian init' first.")]
    NoKeypair(PathBuf),

    #[error("Invalid keypair file: expected {expected} bytes, got {actual}")]
    InvalidKeypair { expected: usize, actual: usize },

    #[error("Keypair already exists at {0}. Delete it first to regenerate.")]
    KeypairExists(PathBuf),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Authentication credentials for a single request.
#[derive(Debug, Clone)]
pub struct AuthCredentials {
    /// Public key in "ed25519p/{hex}" format.
    pub pubkey: String,
    /// Unix timestamp when signature was created.
    pub timestamp: u64,
    /// Hex-encoded signature over "{timestamp}:UPLOAD".
    pub signature: String,
}

/// Authentication manager for Librarian.
#[derive(Clone)]
pub struct Auth {
    signing_key: SigningKey,
}

impl Auth {
    /// Load keypair from file.
    pub fn load(path: &Path) -> Result<Self, AuthError> {
        if !path.exists() {
            return Err(AuthError::NoKeypair(path.to_path_buf()));
        }

        let key_bytes = fs::read(path)?;

        if key_bytes.len() != SECRET_KEY_LENGTH {
            return Err(AuthError::InvalidKeypair {
                expected: SECRET_KEY_LENGTH,
                actual: key_bytes.len(),
            });
        }

        let mut secret_bytes = [0u8; SECRET_KEY_LENGTH];
        secret_bytes.copy_from_slice(&key_bytes);
        let signing_key = SigningKey::from_bytes(&secret_bytes);

        Ok(Self { signing_key })
    }

    /// Initialize a new keypair.
    pub fn init(path: &Path) -> Result<Self, AuthError> {
        if path.exists() {
            return Err(AuthError::KeypairExists(path.to_path_buf()));
        }

        // Create parent directories
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Generate new keypair
        let mut csprng = rand::rngs::OsRng;
        let signing_key = SigningKey::generate(&mut csprng);

        // Save private key
        fs::write(path, signing_key.to_bytes())?;

        // Set restrictive permissions (Unix only)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600);
            fs::set_permissions(path, perms)?;
        }

        Ok(Self { signing_key })
    }

    /// Get the public key in "ed25519p/{hex}" format.
    pub fn public_key(&self) -> String {
        let verifying_key = self.signing_key.verifying_key();
        format!("ed25519p/{}", hex::encode(verifying_key.to_bytes()))
    }

    /// Create authentication credentials for an upload request.
    ///
    /// Signs the message "{timestamp}:UPLOAD" as expected by Archivist.
    pub fn create_credentials(&self) -> AuthCredentials {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Sign "{timestamp}:UPLOAD" as expected by Archivist
        let message = format!("{}:UPLOAD", timestamp);
        let signature = self.signing_key.sign(message.as_bytes());
        let signature_hex = hex::encode(signature.to_bytes());

        AuthCredentials {
            pubkey: self.public_key(),
            timestamp,
            signature: signature_hex,
        }
    }
}

/// Get the default keypair path within a data directory.
pub fn keypair_path(data_dir: &Path) -> PathBuf {
    data_dir.join("auth.key")
}

/// Get the default data directory.
pub fn default_data_dir() -> PathBuf {
    directories::ProjectDirs::from("cc", "riff", "librarian")
        .map(|dirs| dirs.data_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("./librarian-data"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_init_and_load() {
        let temp = TempDir::new().unwrap();
        let key_path = temp.path().join("auth.key");

        // Initialize
        let auth1 = Auth::init(&key_path).unwrap();
        let pubkey1 = auth1.public_key();

        // Load
        let auth2 = Auth::load(&key_path).unwrap();
        let pubkey2 = auth2.public_key();

        // Same public key
        assert_eq!(pubkey1, pubkey2);
        assert!(pubkey1.starts_with("ed25519p/"));
    }

    #[test]
    fn test_create_credentials() {
        let temp = TempDir::new().unwrap();
        let key_path = temp.path().join("auth.key");

        let auth = Auth::init(&key_path).unwrap();
        let creds = auth.create_credentials();

        assert!(creds.pubkey.starts_with("ed25519p/"));
        assert!(creds.timestamp > 0);
        assert!(!creds.signature.is_empty());
        // Signature is 64 bytes = 128 hex chars
        assert_eq!(creds.signature.len(), 128);
    }

    #[test]
    fn test_no_keypair() {
        let result = Auth::load(Path::new("/nonexistent/path/auth.key"));
        assert!(matches!(result, Err(AuthError::NoKeypair(_))));
    }

    #[test]
    fn test_keypair_exists() {
        let temp = TempDir::new().unwrap();
        let key_path = temp.path().join("auth.key");

        Auth::init(&key_path).unwrap();
        let result = Auth::init(&key_path);
        assert!(matches!(result, Err(AuthError::KeypairExists(_))));
    }
}
