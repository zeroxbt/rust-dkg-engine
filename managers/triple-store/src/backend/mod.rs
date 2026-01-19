mod blazegraph;

use std::collections::HashMap;

use async_trait::async_trait;
pub use blazegraph::BlazegraphBackend;

use crate::error::Result;

/// Value from a SPARQL SELECT binding
#[derive(Debug, Clone)]
pub enum SelectValue {
    /// URI/IRI value
    Uri(String),
    /// Literal value with optional datatype and language tag
    Literal {
        value: String,
        datatype: Option<String>,
        language: Option<String>,
    },
    /// Blank node
    BlankNode(String),
}

impl SelectValue {
    /// Get the string value regardless of type
    pub fn as_str(&self) -> &str {
        match self {
            Self::Uri(s) | Self::BlankNode(s) => s,
            Self::Literal { value, .. } => value,
        }
    }
}

/// A row from a SPARQL SELECT query result
#[derive(Debug, Clone)]
pub struct SelectRow {
    pub bindings: HashMap<String, SelectValue>,
}

impl SelectRow {
    /// Get a binding value by variable name
    pub fn get(&self, var: &str) -> Option<&SelectValue> {
        self.bindings.get(var)
    }

    /// Get a binding value as a string
    pub fn get_str(&self, var: &str) -> Option<&str> {
        self.bindings.get(var).map(SelectValue::as_str)
    }
}

/// Result from a SPARQL SELECT query
#[derive(Debug, Clone)]
pub struct SelectResult {
    pub variables: Vec<String>,
    pub rows: Vec<SelectRow>,
}

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

    /// Delete the repository/namespace
    async fn delete_repository(&self) -> Result<()>;

    /* /// Execute a SPARQL CONSTRUCT query
    ///
    /// Returns N-Quads formatted string
    async fn construct(&self, query: &str, timeout_ms: u64) -> Result<String>;

    /// Execute a SPARQL SELECT query
    ///
    /// Returns parsed result bindings
    async fn select(&self, query: &str, timeout_ms: u64) -> Result<SelectResult>;

    /// Execute a SPARQL ASK query
    ///
    /// Returns boolean result
    async fn ask(&self, query: &str, timeout_ms: u64) -> Result<bool>;

    /// Execute a SPARQL UPDATE query (INSERT/DELETE)
    ///
    /// Returns nothing on success
    async fn update(&self, query: &str, timeout_ms: u64) -> Result<()>; */
}
