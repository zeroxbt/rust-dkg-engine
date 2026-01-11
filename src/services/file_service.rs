use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[derive(Error, Debug)]
pub enum FileServiceError {
    #[error("File not found: {0}")]
    FileNotFound(PathBuf),

    #[error("Directory not found: {0}")]
    DirectoryNotFound(PathBuf),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, FileServiceError>;

pub struct FileService {
    data_path: PathBuf,
}

impl FileService {
    pub fn new(data_path: PathBuf) -> Self {
        Self { data_path }
    }

    /// Write data to a file, creating parent directories if needed
    pub async fn write(
        &self,
        dir: impl AsRef<Path>,
        filename: &str,
        data: &[u8],
    ) -> Result<PathBuf> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir).await?;

        let path = dir.join(filename);
        fs::write(&path, data).await?;

        tracing::debug!("Wrote {} bytes to {}", data.len(), path.display());
        Ok(path)
    }

    /// Append data to a file, creating it if it doesn't exist
    pub async fn append(
        &self,
        dir: impl AsRef<Path>,
        filename: &str,
        data: &[u8],
    ) -> Result<PathBuf> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir).await?;

        let path = dir.join(filename);
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        file.write_all(data).await?;

        tracing::debug!("Appended {} bytes to {}", data.len(), path.display());
        Ok(path)
    }

    /// Read a file as bytes
    pub async fn read_bytes(&self, path: impl AsRef<Path>) -> Result<Vec<u8>> {
        let path = path.as_ref();
        fs::read(path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                FileServiceError::FileNotFound(path.to_path_buf())
            } else {
                FileServiceError::Io(e)
            }
        })
    }

    /// Read a file as UTF-8 string
    pub async fn read_string(&self, path: impl AsRef<Path>) -> Result<String> {
        let path = path.as_ref();
        fs::read_to_string(path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                FileServiceError::FileNotFound(path.to_path_buf())
            } else {
                FileServiceError::Io(e)
            }
        })
    }

    /// Read and parse a JSON file
    pub async fn read_json<T: serde::de::DeserializeOwned>(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<T> {
        let data = self.read_bytes(path).await?;
        Ok(serde_json::from_slice(&data)?)
    }

    pub async fn metadata(&self, path: impl AsRef<Path>) -> Result<std::fs::Metadata> {
        let path = path.as_ref();
        tokio::fs::metadata(path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                FileServiceError::FileNotFound(path.to_path_buf())
            } else {
                FileServiceError::Io(e)
            }
        })
    }

    /// Write JSON data to a file
    pub async fn write_json<T: serde::Serialize>(
        &self,
        dir: impl AsRef<Path>,
        filename: &str,
        data: &T,
    ) -> Result<PathBuf> {
        let json = serde_json::to_vec_pretty(data)?;
        self.write(dir, filename, &json).await
    }

    /// List entries in a directory
    pub async fn read_dir(&self, path: impl AsRef<Path>) -> Result<Vec<String>> {
        let path = path.as_ref();
        let mut entries = fs::read_dir(path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                FileServiceError::DirectoryNotFound(path.to_path_buf())
            } else {
                FileServiceError::Io(e)
            }
        })?;

        let mut names = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if let Some(name) = entry.file_name().to_str() {
                names.push(name.to_string());
            }
        }

        Ok(names)
    }

    /// Remove a file. Returns true if file existed and was removed.
    pub async fn remove_file(&self, path: impl AsRef<Path>) -> Result<bool> {
        let path = path.as_ref();
        match fs::remove_file(path).await {
            Ok(_) => {
                tracing::trace!("Removed file: {}", path.display());
                Ok(true)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(FileServiceError::Io(e)),
        }
    }

    /// Remove a directory and all its contents. Returns true if directory existed and was removed.
    pub async fn remove_dir(&self, path: impl AsRef<Path>) -> Result<bool> {
        let path = path.as_ref();
        match fs::remove_dir_all(path).await {
            Ok(_) => {
                tracing::debug!("Removed directory: {}", path.display());
                Ok(true)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(FileServiceError::Io(e)),
        }
    }

    // Path construction helpers - these are cheap and don't need to be methods,
    // but keeping them for API consistency with the JS version

    fn operation_cache_dir(&self) -> PathBuf {
        self.data_path.join("operation_id_cache")
    }

    pub fn operation_request_cache_dir(&self) -> PathBuf {
        self.operation_cache_dir().join("request")
    }

    pub fn operation_response_cache_dir(&self) -> PathBuf {
        self.operation_cache_dir().join("response")
    }

    pub fn operation_request_cache_path(&self, operation_id: &str) -> PathBuf {
        self.operation_request_cache_dir().join(operation_id)
    }

    pub fn operation_response_cache_path(&self, operation_id: &str) -> PathBuf {
        self.operation_response_cache_dir().join(operation_id)
    }

    pub fn pending_storage_cache_dir(&self) -> PathBuf {
        self.data_path.join("pending_storage_cache")
    }

    pub fn pending_storage_path(&self, operation_id: &str) -> PathBuf {
        self.pending_storage_cache_dir().join(operation_id)
    }

    pub fn signature_storage_cache_dir(&self) -> PathBuf {
        self.data_path.join("signature_storage_cache")
    }

    pub fn signature_storage_folder(&self, folder_name: &str) -> PathBuf {
        self.signature_storage_cache_dir().join(folder_name)
    }

    pub fn signature_storage_path(&self, folder_name: &str, operation_id: &str) -> PathBuf {
        self.signature_storage_folder(folder_name)
            .join(operation_id)
    }

    pub fn migration_dir(&self) -> PathBuf {
        self.data_path.join("migrations")
    }

    pub fn update_file_path(&self) -> PathBuf {
        self.data_path.join("UPDATED")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_write_and_read() {
        let temp = TempDir::new().unwrap();
        let service = FileService::new(temp.path().to_path_buf());

        let path = service
            .write(temp.path(), "test.txt", b"hello")
            .await
            .unwrap();
        let content = service.read_string(&path).await.unwrap();
        assert_eq!(content, "hello");
    }

    #[tokio::test]
    async fn test_append() {
        let temp = TempDir::new().unwrap();
        let service = FileService::new(temp.path().to_path_buf());

        service
            .append(temp.path(), "test.txt", b"hello")
            .await
            .unwrap();
        service
            .append(temp.path(), "test.txt", b" world")
            .await
            .unwrap();

        let content = service
            .read_string(temp.path().join("test.txt"))
            .await
            .unwrap();
        assert_eq!(content, "hello world");
    }

    #[tokio::test]
    async fn test_json() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Data {
            value: i32,
        }

        let temp = TempDir::new().unwrap();
        let service = FileService::new(temp.path().to_path_buf());

        let data = Data { value: 42 };
        let path = service
            .write_json(temp.path(), "test.json", &data)
            .await
            .unwrap();
        let loaded: Data = service.read_json(&path).await.unwrap();

        assert_eq!(data, loaded);
    }

    #[tokio::test]
    async fn test_remove() {
        let temp = TempDir::new().unwrap();
        let service = FileService::new(temp.path().to_path_buf());

        let path = service
            .write(temp.path(), "test.txt", b"data")
            .await
            .unwrap();

        let removed = service.remove_file(&path).await.unwrap();
        assert!(removed);

        let removed_again = service.remove_file(&path).await.unwrap();
        assert!(!removed_again);
    }

    #[tokio::test]
    async fn test_read_nonexistent() {
        let temp = TempDir::new().unwrap();
        let service = FileService::new(temp.path().to_path_buf());

        let result = service
            .read_string(temp.path().join("nonexistent.txt"))
            .await;
        assert!(matches!(result, Err(FileServiceError::FileNotFound(_))));
    }
}
