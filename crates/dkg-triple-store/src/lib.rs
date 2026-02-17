mod backend;
mod config;
pub mod error;
mod knowledge_asset;
mod knowledge_collection;
pub mod query;
pub mod rdf;
pub(crate) mod sparql;
pub mod types;
use std::{path::Path, sync::Arc, time::Duration};

use backend::{BlazegraphBackend, OxigraphBackend, TripleStoreBackend};
pub use config::{DKG_REPOSITORY, TimeoutConfig, TripleStoreBackendType, TripleStoreManagerConfig};
use error::{Result, TripleStoreError};
pub use rdf::{
    compare_js_default_string_order, extract_subject, group_triples_by_subject,
    parse_metadata_from_triples,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
pub use types::GraphVisibility;

#[cfg(test)]
mod tests;

/// Triple Store Manager
///
/// Provides high-level operations for managing RDF data in the DKG triple store.
/// Supports knowledge collection and asset operations with SPARQL.
pub struct TripleStoreManager {
    pub(crate) backend: Box<dyn TripleStoreBackend>,
    pub(crate) config: TripleStoreManagerConfig,
    /// Semaphore for limiting concurrent operations
    concurrency_limiter: Arc<Semaphore>,
}

impl TripleStoreManager {
    /// Create a new Triple Store Manager
    ///
    /// Creates the appropriate backend based on configuration and ensures
    /// the repository/store is ready.
    ///
    /// # Arguments
    /// * `config` - Triple store configuration
    /// * `data_path` - Path for Oxigraph persistent storage (ignored for Blazegraph)
    pub async fn connect(config: &TripleStoreManagerConfig, data_path: &Path) -> Result<Self> {
        let backend: Box<dyn TripleStoreBackend> = match config.backend {
            TripleStoreBackendType::Blazegraph => Box::new(BlazegraphBackend::new(config.clone())?),
            TripleStoreBackendType::Oxigraph => {
                // Create full path with repository name
                let store_path = data_path.join(DKG_REPOSITORY);

                // Ensure the directory exists
                if let Some(parent) = store_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        TripleStoreError::Other(format!(
                            "Failed to create Oxigraph store directory: {}",
                            e
                        ))
                    })?;
                }

                Box::new(OxigraphBackend::open(store_path)?)
            }
        };

        // Initialize concurrency limiter
        let max_concurrent = config.max_concurrent_operations.max(1);
        if max_concurrent != config.max_concurrent_operations {
            tracing::warn!(
                configured = config.max_concurrent_operations,
                effective = max_concurrent,
                "Triple store max_concurrent_operations too low; clamped"
            );
        }
        tracing::info!(
            max_concurrent = max_concurrent,
            "Triple store concurrency limiter initialized"
        );
        let concurrency_limiter = Arc::new(Semaphore::new(max_concurrent));

        let manager = Self {
            backend,
            config: config.clone(),
            concurrency_limiter,
        };

        // For Blazegraph, attempt connection with retries
        // For Oxigraph, this is essentially a no-op (always healthy)
        if config.backend == TripleStoreBackendType::Blazegraph {
            manager.connect_with_retry().await?;
        }

        // Ensure the repository exists (no-op for Oxigraph)
        manager.ensure_repository().await?;

        Ok(manager)
    }

    /// Connect to triple store with retry logic
    async fn connect_with_retry(&self) -> Result<()> {
        let mut attempts = 0;

        loop {
            attempts += 1;

            match self.backend.health_check().await {
                Ok(true) => {
                    tracing::info!(
                        backend = %self.backend.name(),
                        url = %self.config.url,
                        "Connected to triple store"
                    );
                    return Ok(());
                }
                Ok(false) => {
                    tracing::warn!(
                        backend = %self.backend.name(),
                        attempt = attempts,
                        "Triple store health check returned false"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        backend = %self.backend.name(),
                        attempt = attempts,
                        error = %e,
                        "Failed to connect to triple store"
                    );
                }
            }

            if attempts >= self.config.connect_max_retries {
                return Err(TripleStoreError::ConnectionFailed { attempts });
            }

            tokio::time::sleep(self.config.connect_retry_frequency()).await;
        }
    }

    /// Ensure the repository exists, creating it if necessary
    pub async fn ensure_repository(&self) -> Result<()> {
        if !self.backend.repository_exists().await? {
            tracing::info!(
                repository = %DKG_REPOSITORY,
                "Repository does not exist, creating..."
            );
            self.backend.create_repository().await?;
        }
        Ok(())
    }

    // ========== Internal Backend Wrappers (with concurrency limiting) ==========

    async fn acquire_permit(&self) -> Result<OwnedSemaphorePermit> {
        self.concurrency_limiter
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| TripleStoreError::SemaphoreClosed)
    }

    /// Execute a SPARQL UPDATE with concurrency limiting
    pub(crate) async fn backend_update(&self, query: &str, timeout: Duration) -> Result<()> {
        let _permit = self.acquire_permit().await?;
        self.backend.update(query, timeout).await
    }

    #[cfg(test)]
    pub async fn raw_update_for_tests(&self, query: &str) -> Result<()> {
        // Convenience wrapper for tests that need to inject raw triples.
        self.backend_update(query, self.config.timeouts.insert_timeout())
            .await
    }

    /// Execute a SPARQL CONSTRUCT with concurrency limiting
    pub(crate) async fn backend_construct(&self, query: &str, timeout: Duration) -> Result<String> {
        let _permit = self.acquire_permit().await?;
        self.backend.construct(query, timeout).await
    }

    /// Execute a SPARQL ASK with concurrency limiting
    pub(crate) async fn backend_ask(&self, query: &str, timeout: Duration) -> Result<bool> {
        let _permit = self.acquire_permit().await?;
        self.backend.ask(query, timeout).await
    }

    /// Execute a SPARQL SELECT with concurrency limiting
    pub(crate) async fn backend_select(&self, query: &str, timeout: Duration) -> Result<String> {
        let _permit = self.acquire_permit().await?;
        self.backend.select(query, timeout).await
    }
}

#[cfg(test)]
impl TripleStoreManager {
    pub fn from_backend_for_tests(
        backend: Box<dyn TripleStoreBackend>,
        config: TripleStoreManagerConfig,
    ) -> Self {
        let max_concurrent = config.max_concurrent_operations.max(1);
        let concurrency_limiter = Arc::new(Semaphore::new(max_concurrent));
        Self {
            backend,
            config,
            concurrency_limiter,
        }
    }
}
