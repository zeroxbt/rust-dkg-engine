use std::{path::Path, time::Duration};

use async_trait::async_trait;
use oxigraph::{
    sparql::{QueryResults, SparqlEvaluator},
    store::Store,
};

use super::TripleStoreBackend;
use crate::error::{Result, TripleStoreError};

/// Oxigraph embedded triple store backend
///
/// Provides a Rust-native triple store with significantly lower latency
/// than HTTP-based backends like Blazegraph (~1-5ms vs ~50-100ms).
pub struct OxigraphBackend {
    store: Store,
}

impl OxigraphBackend {
    /// Create a new Oxigraph backend with persistent storage
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let store = Store::open(&path).map_err(|e| {
            TripleStoreError::Other(format!("Failed to open Oxigraph store: {}", e))
        })?;

        tracing::info!(
            path = %path.as_ref().display(),
            "Opened Oxigraph persistent store"
        );

        Ok(Self { store })
    }

    /// Create a new in-memory Oxigraph backend (for testing)
    pub fn in_memory() -> Result<Self> {
        let store = Store::new().map_err(|e| {
            TripleStoreError::Other(format!("Failed to create in-memory Oxigraph store: {}", e))
        })?;

        tracing::info!("Created in-memory Oxigraph store");

        Ok(Self { store })
    }

    /// Get direct access to the underlying store for advanced operations
    pub fn store(&self) -> &Store {
        &self.store
    }
}

#[async_trait]
impl TripleStoreBackend for OxigraphBackend {
    fn name(&self) -> &'static str {
        "oxigraph"
    }

    async fn health_check(&self) -> Result<bool> {
        // Oxigraph is embedded, so if we have a store, it's healthy
        // Do a simple query to verify it's working
        let result = SparqlEvaluator::new()
            .parse_query("ASK { ?s ?p ?o }")
            .map_err(|e| {
                TripleStoreError::Other(format!("Health check query parse failed: {}", e))
            })?
            .on_store(&self.store)
            .execute()
            .map_err(|e| TripleStoreError::Other(format!("Health check query failed: {}", e)))?;

        match result {
            QueryResults::Boolean(_) => Ok(true),
            _ => Ok(false),
        }
    }

    async fn repository_exists(&self) -> Result<bool> {
        // Oxigraph doesn't have the concept of separate repositories/namespaces
        // within a single store. The store itself is the "repository".
        // If we have a store, it exists.
        Ok(true)
    }

    async fn create_repository(&self) -> Result<()> {
        // No-op for Oxigraph - the store is created when opened
        Ok(())
    }

    async fn delete_repository(&self) -> Result<()> {
        // Clear all data from the store
        self.store
            .clear()
            .map_err(|e| TripleStoreError::Other(format!("Failed to clear store: {}", e)))?;

        tracing::info!("Cleared all data from Oxigraph store");
        Ok(())
    }

    async fn update(&self, query: &str, _timeout: Duration) -> Result<()> {
        // Parse the query first (CPU-bound, but fast)
        let prepared = SparqlEvaluator::new().parse_update(query).map_err(|e| {
            TripleStoreError::InvalidQuery {
                reason: format!("Failed to parse SPARQL UPDATE: {}", e),
            }
        })?;

        // Execute on blocking thread pool since Oxigraph's update involves disk I/O
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            prepared
                .on_store(&store)
                .execute()
                .map_err(|e| TripleStoreError::Other(format!("SPARQL UPDATE failed: {}", e)))
        })
        .await
        .map_err(|e| TripleStoreError::Other(format!("Task join error: {}", e)))??;

        Ok(())
    }

    async fn construct(&self, query: &str, _timeout: Duration) -> Result<String> {
        // Parse the query
        let prepared = SparqlEvaluator::new().parse_query(query).map_err(|e| {
            TripleStoreError::InvalidQuery {
                reason: format!("Failed to parse SPARQL CONSTRUCT: {}", e),
            }
        })?;

        // Execute on blocking thread pool
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let result = prepared
                .on_store(&store)
                .execute()
                .map_err(|e| TripleStoreError::Other(format!("SPARQL CONSTRUCT failed: {}", e)))?;

            match result {
                QueryResults::Graph(triples) => {
                    let mut output = Vec::new();
                    for triple_result in triples {
                        let triple = triple_result.map_err(|e| {
                            TripleStoreError::Other(format!("Failed to read triple: {}", e))
                        })?;

                        // Serialize to N-Triples format
                        output.push(format!(
                            "{} {} {} .",
                            triple.subject, triple.predicate, triple.object
                        ));
                    }
                    Ok(output.join("\n"))
                }
                _ => Err(TripleStoreError::Other(
                    "Expected CONSTRUCT to return graph results".to_string(),
                )),
            }
        })
        .await
        .map_err(|e| TripleStoreError::Other(format!("Task join error: {}", e)))?
    }

    async fn ask(&self, query: &str, _timeout: Duration) -> Result<bool> {
        // Parse the query
        let prepared = SparqlEvaluator::new().parse_query(query).map_err(|e| {
            TripleStoreError::InvalidQuery {
                reason: format!("Failed to parse SPARQL ASK: {}", e),
            }
        })?;

        // Execute on blocking thread pool
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let result = prepared
                .on_store(&store)
                .execute()
                .map_err(|e| TripleStoreError::Other(format!("SPARQL ASK failed: {}", e)))?;

            match result {
                QueryResults::Boolean(value) => Ok(value),
                _ => Err(TripleStoreError::Other(
                    "Expected ASK to return boolean result".to_string(),
                )),
            }
        })
        .await
        .map_err(|e| TripleStoreError::Other(format!("Task join error: {}", e)))?
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[tokio::test]
    async fn test_in_memory_store() {
        let backend = OxigraphBackend::in_memory().unwrap();

        // Health check should pass
        assert!(backend.health_check().await.unwrap());

        // Repository should "exist"
        assert!(backend.repository_exists().await.unwrap());
    }

    #[tokio::test]
    async fn test_insert_and_query() {
        let backend = OxigraphBackend::in_memory().unwrap();

        // Insert some data
        let insert_query = r#"
            PREFIX ex: <http://example.org/>
            INSERT DATA {
                GRAPH <http://example.org/graph1> {
                    ex:subject1 ex:predicate1 "object1" .
                    ex:subject2 ex:predicate2 "object2" .
                }
            }
        "#;

        backend.update(insert_query, Duration::from_secs(10)).await.unwrap();

        // Verify data was inserted by checking the store directly
        let count = SparqlEvaluator::new()
            .parse_query("SELECT (COUNT(*) as ?count) WHERE { GRAPH ?g { ?s ?p ?o } }")
            .unwrap()
            .on_store(&backend.store)
            .execute()
            .unwrap();

        if let QueryResults::Solutions(mut solutions) = count {
            let solution = solutions.next().unwrap().unwrap();
            let count_value = solution.get("count").unwrap();
            // Should have 2 triples
            assert!(count_value.to_string().contains('2'));
        } else {
            panic!("Expected solutions");
        }
    }

    #[tokio::test]
    async fn test_named_graphs() {
        let backend = OxigraphBackend::in_memory().unwrap();

        // Insert into multiple named graphs (like KC public/private)
        let insert_query = r#"
            PREFIX ex: <http://example.org/>
            INSERT DATA {
                GRAPH <did:dkg:test/1/public> {
                    ex:asset1 ex:name "Public Asset" .
                }
                GRAPH <did:dkg:test/1/private> {
                    ex:asset1 ex:secret "Private Data" .
                }
            }
        "#;

        backend.update(insert_query, Duration::from_secs(10)).await.unwrap();

        // Query public graph only
        let result = SparqlEvaluator::new()
            .parse_query(
                r#"
                SELECT ?name WHERE {
                    GRAPH <did:dkg:test/1/public> {
                        ?s <http://example.org/name> ?name .
                    }
                }
            "#,
            )
            .unwrap()
            .on_store(&backend.store)
            .execute()
            .unwrap();

        if let QueryResults::Solutions(mut solutions) = result {
            let solution = solutions.next().unwrap().unwrap();
            let name = solution.get("name").unwrap().to_string();
            assert!(name.contains("Public Asset"));
        }
    }

    #[tokio::test]
    async fn test_clear_store() {
        let backend = OxigraphBackend::in_memory().unwrap();

        // Insert data
        backend
            .update(
                "INSERT DATA { GRAPH <g:1> { <s:1> <p:1> <o:1> . } }",
                Duration::from_secs(10),
            )
            .await
            .unwrap();

        // Clear
        backend.delete_repository().await.unwrap();

        // Verify empty
        let result = SparqlEvaluator::new()
            .parse_query("ASK { GRAPH ?g { ?s ?p ?o } }")
            .unwrap()
            .on_store(&backend.store)
            .execute()
            .unwrap();

        if let QueryResults::Boolean(exists) = result {
            assert!(!exists);
        }
    }
}
