// use base64::{engine::general_purpose, Engine as _};
// use rand::rngs::OsRng;
// use rsa::{pkcs8::EncodePrivateKey, RsaPrivateKey};
use std::path::PathBuf;

use libp2p::identity;
use tokio::{fs, io};

use crate::error::{NetworkError, Result};

const LIBP2P_KEY_FILENAME: &str = "private_key";
pub(super) struct KeyManager;

impl KeyManager {
    fn get_key_path(data_folder_path: &PathBuf) -> PathBuf {
        data_folder_path.join(LIBP2P_KEY_FILENAME)
    }

    async fn ensure_key_directory_exists(data_folder_path: &PathBuf) -> io::Result<()> {
        let key_path = Self::get_key_path(data_folder_path);
        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        Ok(())
    }

    async fn save_private_key_to_file(
        key: &libp2p::identity::ed25519::Keypair,
        data_folder_path: &PathBuf,
    ) -> io::Result<()> {
        Self::ensure_key_directory_exists(data_folder_path).await?;
        // let der_format = key
        // .to_pkcs8_der()
        // .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
        // .to_bytes();
        //
        // let base64_encoded = general_purpose::STANDARD.encode(&der_format);
        fs::write(Self::get_key_path(data_folder_path), key.to_bytes()).await
    }

    async fn read_private_key_from_file(
        data_folder_path: &PathBuf,
    ) -> io::Result<libp2p::identity::ed25519::Keypair> {
        println!("reading private key from file...");
        let mut key_bytes = fs::read(Self::get_key_path(data_folder_path)).await?;
        // let mut der_format = general_purpose::STANDARD
        // .decode(base64_encoded)
        // .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        println!("Creating keypair...");
        libp2p::identity::ed25519::Keypair::try_from_bytes(&mut key_bytes)
            .map_err(|e| io::Error::other(e.to_string()))
    }

    pub(super) async fn generate_or_load_key(
        data_folder_path: &PathBuf,
    ) -> Result<libp2p::identity::Keypair> {
        match Self::read_private_key_from_file(data_folder_path).await {
            Ok(pk) => Ok(pk.into()),
            Err(error) => {
                tracing::debug!("No existing key found ({}), generating new key", error);

                let new_key = identity::Keypair::generate_ed25519();

                // Extract ed25519 keypair for saving
                let ed25519_key = new_key
                    .clone()
                    .try_into_ed25519()
                    .map_err(NetworkError::KeyConversion)?;

                Self::save_private_key_to_file(&ed25519_key, data_folder_path).await?;

                Ok(new_key)
            }
        }
    }
}
