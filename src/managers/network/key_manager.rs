use std::path::Path;

use libp2p::identity;
use tokio::{fs, io};

use crate::managers::network::error::{NetworkError, Result};

/// Manages network identity keys (libp2p Ed25519 keypairs).
///
/// Provides functionality to load existing keys from disk or generate new ones.
/// Keys are stored as raw Ed25519 bytes.
pub(crate) struct KeyManager;

impl KeyManager {
    /// Ensure the parent directory for a key file exists.
    async fn ensure_key_directory_exists(key_path: &Path) -> io::Result<()> {
        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        Ok(())
    }

    /// Save an Ed25519 keypair to a file.
    async fn save_private_key_to_file(
        key: &libp2p::identity::ed25519::Keypair,
        key_path: &Path,
    ) -> io::Result<()> {
        Self::ensure_key_directory_exists(key_path).await?;
        fs::write(key_path, key.to_bytes()).await
    }

    /// Read an Ed25519 keypair from a file.
    async fn read_private_key_from_file(
        key_path: &Path,
    ) -> io::Result<libp2p::identity::ed25519::Keypair> {
        tracing::trace!("Reading private key from file: {}", key_path.display());
        let mut key_bytes = fs::read(key_path).await?;
        tracing::trace!("Creating keypair from bytes");
        libp2p::identity::ed25519::Keypair::try_from_bytes(&mut key_bytes)
            .map_err(|e| io::Error::other(e.to_string()))
    }

    /// Load an existing keypair from the specified path, or generate and save a new one.
    ///
    /// # Arguments
    /// * `key_path` - Full path to the key file (e.g., "data/network/private_key")
    ///
    /// # Returns
    /// The loaded or newly generated keypair.
    pub(crate) async fn load_or_generate(key_path: &Path) -> Result<libp2p::identity::Keypair> {
        match Self::read_private_key_from_file(key_path).await {
            Ok(pk) => {
                tracing::info!("Loaded existing network key from {}", key_path.display());
                Ok(pk.into())
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                tracing::info!(
                    "No existing key found at {}, generating new key",
                    key_path.display()
                );

                let new_key = identity::Keypair::generate_ed25519();

                // Extract ed25519 keypair for saving
                let ed25519_key = new_key
                    .clone()
                    .try_into_ed25519()
                    .map_err(NetworkError::KeyConversion)?;

                Self::save_private_key_to_file(&ed25519_key, key_path).await?;
                tracing::info!("Saved new network key to {}", key_path.display());

                Ok(new_key)
            }
            Err(error) => {
                tracing::error!(
                    "Failed to load existing network key from {}: {}",
                    key_path.display(),
                    error
                );
                Err(NetworkError::PeerIdIo(error))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use tokio::fs;

    use super::*;

    #[tokio::test]
    async fn load_or_generate_fails_for_invalid_existing_key() {
        let temp_dir = TempDir::new().expect("temp dir");
        let key_path = temp_dir.path().join("network/private_key");

        fs::create_dir_all(
            key_path
                .parent()
                .expect("private key path should have a parent directory"),
        )
        .await
        .expect("create key dir");
        fs::write(&key_path, b"invalid-private-key-bytes")
            .await
            .expect("write invalid key bytes");

        let result = KeyManager::load_or_generate(&key_path).await;
        assert!(matches!(result, Err(NetworkError::PeerIdIo(_))));
    }

    #[tokio::test]
    async fn load_or_generate_creates_and_reuses_key() {
        let temp_dir = TempDir::new().expect("temp dir");
        let key_path = temp_dir.path().join("network/private_key");

        let first = KeyManager::load_or_generate(&key_path)
            .await
            .expect("first key generation should succeed");
        let second = KeyManager::load_or_generate(&key_path)
            .await
            .expect("second key load should succeed");

        assert_eq!(first.public(), second.public());
    }
}
