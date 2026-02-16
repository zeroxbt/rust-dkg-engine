mod blazegraph;
mod oxigraph_backend;

use std::time::Duration;

use async_trait::async_trait;
pub use blazegraph::BlazegraphBackend;
pub use oxigraph_backend::OxigraphBackend;

use crate::error::Result;

/// Trait for triple store backends
///
/// Implementations provide the low-level SPARQL execution against
/// specific triple store systems (Blazegraph, Fuseki, GraphDB, etc.)
#[async_trait]
pub trait TripleStoreBackend: Send + Sync {
    /// Backend name for logging/debugging
    fn name(&self) -> &'static str;

    /// Health check - verify the triple store is reachable
    async fn health_check(&self) -> Result<bool>;

    /// Check if the configured repository/namespace exists
    async fn repository_exists(&self) -> Result<bool>;

    /// Create the repository/namespace with appropriate settings
    async fn create_repository(&self) -> Result<()>;

    #[cfg(test)]
    /// Delete the repository/namespace
    async fn delete_repository(&self) -> Result<()>;

    /// Execute a SPARQL UPDATE query (INSERT/DELETE)
    ///
    /// Returns nothing on success
    async fn update(&self, query: &str, timeout: Duration) -> Result<()>;

    /// Execute a SPARQL CONSTRUCT query
    ///
    /// Returns RDF lines (N-Triples/N-Quads)
    async fn construct(&self, query: &str, timeout: Duration) -> Result<String>;

    /// Execute a SPARQL ASK query
    ///
    /// Returns true if the pattern exists, false otherwise
    async fn ask(&self, query: &str, timeout: Duration) -> Result<bool>;

    /// Execute a SPARQL SELECT query
    ///
    /// Returns SPARQL results JSON as a string
    async fn select(&self, query: &str, timeout: Duration) -> Result<String>;
}
