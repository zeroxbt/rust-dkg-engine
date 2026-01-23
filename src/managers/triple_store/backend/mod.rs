mod blazegraph;
mod oxigraph_backend;

use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
pub(crate) use blazegraph::BlazegraphBackend;
pub(crate) use oxigraph_backend::OxigraphBackend;

use crate::managers::triple_store::error::Result;

/// Value from a SPARQL SELECT binding
#[derive(Debug, Clone)]
pub(crate) enum SelectValue {
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
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Uri(s) | Self::BlankNode(s) => s,
            Self::Literal { value, .. } => value,
        }
    }
}

/// A row from a SPARQL SELECT query result
#[derive(Debug, Clone)]
pub(crate) struct SelectRow {
    pub bindings: HashMap<String, SelectValue>,
}

impl SelectRow {
    /// Get a binding value by variable name
    pub(crate) fn get(&self, var: &str) -> Option<&SelectValue> {
        self.bindings.get(var)
    }

    /// Get a binding value as a string
    pub(crate) fn get_str(&self, var: &str) -> Option<&str> {
        self.bindings.get(var).map(SelectValue::as_str)
    }
}

/// Result from a SPARQL SELECT query
#[derive(Debug, Clone)]
pub(crate) struct SelectResult {
    pub variables: Vec<String>,
    pub rows: Vec<SelectRow>,
}

/// Trait for triple store backends
///
/// Implementations provide the low-level SPARQL execution against
/// specific triple store systems (Blazegraph, Fuseki, GraphDB, etc.)
#[async_trait]
pub(crate) trait TripleStoreBackend: Send + Sync {
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

    /// Execute a SPARQL UPDATE query (INSERT/DELETE)
    ///
    /// Returns nothing on success
    async fn update(&self, query: &str, timeout: Duration) -> Result<()>;

    /// Execute a SPARQL CONSTRUCT query
    ///
    /// Returns N-Quads/N-Triples serialized RDF
    async fn construct(&self, query: &str, timeout: Duration) -> Result<String>;

    /// Execute a SPARQL ASK query
    ///
    /// Returns true if the pattern exists, false otherwise
    async fn ask(&self, query: &str, timeout: Duration) -> Result<bool>;
}
