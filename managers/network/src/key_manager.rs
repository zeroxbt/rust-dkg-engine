use base64::{engine::general_purpose, Engine as _};
use rand::rngs::OsRng;
use rsa::{pkcs8::EncodePrivateKey, RsaPrivateKey};
use std::path::PathBuf;
use tokio::fs;
use tokio::io;

const LIBP2P_KEY_FILENAME: &str = "private_key";

pub(super) struct KeyManager;

impl KeyManager {
    fn get_key_path(data_folder_path: String) -> PathBuf {
        PathBuf::from(format!("./{}/libp2p", data_folder_path)).join(LIBP2P_KEY_FILENAME)
    }

    async fn ensure_key_directory_exists(data_folder_path: String) -> io::Result<()> {
        let key_path = Self::get_key_path(data_folder_path);
        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        Ok(())
    }

    async fn save_private_key_to_file(
        key: &RsaPrivateKey,
        data_folder_path: String,
    ) -> io::Result<()> {
        Self::ensure_key_directory_exists(data_folder_path.clone()).await?;
        let der_format = key
            .to_pkcs8_der()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
            .to_bytes();

        let base64_encoded = general_purpose::STANDARD.encode(&der_format);
        fs::write(Self::get_key_path(data_folder_path), base64_encoded).await
    }

    async fn read_private_key_from_file(
        data_folder_path: String,
    ) -> io::Result<libp2p::identity::Keypair> {
        let base64_encoded = fs::read_to_string(Self::get_key_path(data_folder_path)).await?;
        let mut der_format = general_purpose::STANDARD
            .decode(base64_encoded)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        libp2p::identity::Keypair::rsa_from_pkcs8(&mut der_format)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    pub(super) async fn generate_or_load_key(
        data_folder_path: String,
    ) -> io::Result<libp2p::identity::Keypair> {
        match Self::read_private_key_from_file(data_folder_path.clone()).await {
            Ok(pk) => Ok(pk),
            Err(_) => {
                let new_key = RsaPrivateKey::new(&mut OsRng, 2048)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                let mut key_der = EncodePrivateKey::to_pkcs8_der(&new_key)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
                    .to_bytes();
                let key = libp2p::identity::Keypair::rsa_from_pkcs8(&mut key_der)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                Self::save_private_key_to_file(&new_key, data_folder_path).await?;
                Ok(key)
            }
        }
    }
}
