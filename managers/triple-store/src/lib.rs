pub mod backend;
pub mod config;
pub mod error;
pub mod query;
pub mod types;

use std::time::Duration;

use backend::{BlazegraphBackend, TripleStoreBackend};
use error::{Result, TripleStoreError};
use query::{named_graphs, predicates};

/// Format a unix timestamp as ISO 8601 datetime string
fn format_unix_timestamp(timestamp: u64) -> String {
    use std::time::UNIX_EPOCH;

    let duration = Duration::from_secs(timestamp);
    let datetime = UNIX_EPOCH + duration;

    // Format as ISO 8601 (simplified - not using chrono to avoid dependency)
    // Output format: "2024-01-15T10:30:00.000Z"
    if let Ok(duration_since_epoch) = datetime.duration_since(UNIX_EPOCH) {
        let secs = duration_since_epoch.as_secs();
        let days_since_epoch = secs / 86400;
        let time_of_day = secs % 86400;

        let hours = time_of_day / 3600;
        let minutes = (time_of_day % 3600) / 60;
        let seconds = time_of_day % 60;

        // Calculate year, month, day from days since epoch (1970-01-01)
        let (year, month, day) = days_to_ymd(days_since_epoch as i64);

        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.000Z",
            year, month, day, hours, minutes, seconds
        )
    } else {
        "1970-01-01T00:00:00.000Z".to_string()
    }
}

/// Convert days since Unix epoch to (year, month, day)
fn days_to_ymd(days: i64) -> (i32, u32, u32) {
    // Algorithm from Howard Hinnant
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    (y as i32, m, d)
}

// Re-export commonly used types for convenience
pub use backend::{SelectResult, SelectRow, SelectValue};
pub use config::TripleStoreManagerConfig;
pub use error::TripleStoreError as Error;
pub use types::Visibility;

/// Metadata for a knowledge collection
#[derive(Debug, Clone)]
pub struct KnowledgeCollectionMetadata {
    pub published_by: String,
    pub published_at_block: u64,
    pub publish_tx: String,
    pub publish_time: u64,
    pub block_time: u64,
}

/// Triple Store Manager
///
/// Provides high-level operations for managing RDF data in the DKG triple store.
/// Supports knowledge collection and asset operations with SPARQL.
pub struct TripleStoreManager {
    backend: Box<dyn TripleStoreBackend>,
    config: TripleStoreManagerConfig,
}

impl TripleStoreManager {
    /// Create a new Triple Store Manager
    ///
    /// Establishes connection to the triple store with retry logic.
    pub async fn new(config: &TripleStoreManagerConfig) -> Result<Self> {
        let backend = Box::new(BlazegraphBackend::new(config.clone())?);

        let manager = Self {
            backend,
            config: config.clone(),
        };

        // Attempt connection with retries
        manager.connect_with_retry().await?;

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

            tokio::time::sleep(Duration::from_millis(
                self.config.connect_retry_frequency_ms,
            ))
            .await;
        }
    }

    /// Health check for the triple store
    pub async fn health_check(&self) -> Result<bool> {
        self.backend.health_check().await
    }

    /// Ensure the repository exists, creating it if necessary
    pub async fn ensure_repository(&self) -> Result<()> {
        if !self.backend.repository_exists().await? {
            tracing::info!(
                repository = %self.config.repository,
                "Repository does not exist, creating..."
            );
            self.backend.create_repository().await?;
        }
        Ok(())
    }

    /*  // ========== Knowledge Collection Operations ==========

    /// Insert a knowledge collection into the triple store
    ///
    /// Inserts triples into:
    /// - Named graphs for public/private assertions
    /// - Metadata graph with KC metadata
    /// - Current graph with named graph references
    pub async fn insert_knowledge_collection(
        &self,
        ual: &str,
        public_nquads: &str,
        private_nquads: Option<&str>,
        metadata: &KnowledgeCollectionMetadata,
    ) -> Result<()> {
        let mut query = String::from("INSERT DATA {\n");

        // Public named graph
        if !public_nquads.is_empty() {
            let public_graph = format!("{ual}/public");
            query.push_str(&format!("  GRAPH <{public_graph}> {{\n"));
            for line in public_nquads.lines() {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    query.push_str(&format!("    {trimmed}\n"));
                }
            }
            query.push_str("  }\n");

            // Current graph reference
            query.push_str(&format!(
                "  GRAPH <{}> {{\n    <{}> <{}> <{}> .\n  }}\n",
                named_graphs::CURRENT,
                named_graphs::CURRENT,
                predicates::HAS_NAMED_GRAPH,
                public_graph
            ));
        }

        // Private named graph
        if let Some(private) = private_nquads
            && !private.is_empty()
        {
            let private_graph = format!("{ual}/private");
            query.push_str(&format!("  GRAPH <{private_graph}> {{\n"));
            for line in private.lines() {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    query.push_str(&format!("    {trimmed}\n"));
                }
            }
            query.push_str("  }\n");

            // Current graph reference
            query.push_str(&format!(
                "  GRAPH <{}> {{\n    <{}> <{}> <{}> .\n  }}\n",
                named_graphs::CURRENT,
                named_graphs::CURRENT,
                predicates::HAS_NAMED_GRAPH,
                private_graph
            ));
        }

        // Metadata graph
        query.push_str(&format!("  GRAPH <{}> {{\n", named_graphs::METADATA));
        query.push_str(&format!(
            "    <{ual}> <{}> <did:dkg:publisherKey/{}> .\n",
            predicates::PUBLISHED_BY,
            metadata.published_by
        ));
        query.push_str(&format!(
            "    <{ual}> <{}> \"{}\" .\n",
            predicates::PUBLISHED_AT_BLOCK,
            metadata.published_at_block
        ));
        query.push_str(&format!(
            "    <{ual}> <{}> \"{}\" .\n",
            predicates::PUBLISH_TX,
            metadata.publish_tx
        ));
        // Convert unix timestamp to ISO 8601 datetime
        let publish_time_iso = format_unix_timestamp(metadata.publish_time);
        query.push_str(&format!(
            "    <{ual}> <{}> \"{}\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n",
            predicates::PUBLISH_TIME,
            publish_time_iso
        ));
        let block_time_iso = format_unix_timestamp(metadata.block_time);
        query.push_str(&format!(
            "    <{ual}> <{}> \"{}\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n",
            predicates::BLOCK_TIME,
            block_time_iso
        ));
        query.push_str("  }\n");

        query.push_str("}\n");

        self.backend
            .update(&query, self.config.timeouts.insert_ms)
            .await?;

        tracing::debug!(ual = %ual, "Inserted knowledge collection");
        Ok(())
    } */

    /* /// Get knowledge collection from unified graph
    ///
    /// Returns N-Quads string with triples matching the UAL prefix
    pub async fn get_knowledge_collection(
        &self,
        ual: &str,
        visibility: Visibility,
    ) -> Result<String> {
        let filter = match visibility {
            Visibility::Public => format!(
                r#"FILTER(STRSTARTS(STR(?ual), "{ual}/"))
        FILTER NOT EXISTS {{ << ?s ?p ?o >> <{label}> "private" . }}"#,
                label = predicates::LABEL
            ),
            Visibility::Private | Visibility::All => {
                format!(r#"FILTER(STRSTARTS(STR(?ual), "{ual}/"))"#)
            }
        };

        let query = format!(
            r#"CONSTRUCT {{ ?s ?p ?o . }}
WHERE {{
    GRAPH <{unified}> {{
        << ?s ?p ?o >> <{ual_pred}> ?ual .
        {filter}
    }}
}}
ORDER BY ?s"#,
            unified = named_graphs::UNIFIED,
            ual_pred = predicates::UAL,
        );

        self.backend
            .construct(&query, self.config.timeouts.query_ms)
            .await
    } */

    /*  /// Get knowledge collection from named graphs
    ///
    /// Returns triples from the specific public/private named graphs
    pub async fn get_knowledge_collection_named_graphs(
        &self,
        ual: &str,
        visibility: Visibility,
    ) -> Result<String> {
        let graphs: Vec<String> = match visibility {
            Visibility::Public => vec![format!("{ual}/public")],
            Visibility::Private => vec![format!("{ual}/private")],
            Visibility::All => vec![format!("{ual}/public"), format!("{ual}/private")],
        };

        let graph_patterns: Vec<String> = graphs
            .iter()
            .map(|g| format!("GRAPH <{g}> {{ ?s ?p ?o . }}"))
            .collect();

        let query = format!(
            r#"CONSTRUCT {{ ?s ?p ?o . }}
WHERE {{
    {{ {} }}
}}"#,
            graph_patterns.join(" } UNION { ")
        );

        self.backend
            .construct(&query, self.config.timeouts.query_ms)
            .await
    }

    /// Delete knowledge collection from unified graph
    ///
    /// Uses annotation count to only delete triples that belong exclusively
    /// to this knowledge collection (not shared with others)
    pub async fn delete_knowledge_collection(&self, ual: &str) -> Result<()> {
        let query = format!(
            r#"DELETE {{
    GRAPH <{unified}> {{
        ?s ?p ?o .
        << ?s ?p ?o >> ?annotationPredicate ?annotationValue .
    }}
}}
WHERE {{
    GRAPH <{unified}> {{
        << ?s ?p ?o >> <{ual_pred}> ?annotationValue .
    }}
    FILTER(STRSTARTS(STR(?annotationValue), "{ual}/"))
    {{
        SELECT ?s ?p ?o (COUNT(?annotationValue) AS ?annotationCount)
        WHERE {{
            GRAPH <{unified}> {{
                << ?s ?p ?o >> <{ual_pred}> ?annotationValue .
            }}
        }}
        GROUP BY ?s ?p ?o
        HAVING(?annotationCount = 1)
    }}
}}"#,
            unified = named_graphs::UNIFIED,
            ual_pred = predicates::UAL,
        );

        self.backend
            .update(&query, self.config.timeouts.insert_ms)
            .await?;

        tracing::debug!(ual = %ual, "Deleted knowledge collection from unified graph");
        Ok(())
    }

    /// Check if knowledge collection exists in unified graph
    pub async fn knowledge_collection_exists(&self, ual: &str) -> Result<bool> {
        let query = format!(
            r#"ASK WHERE {{
    GRAPH <{unified}> {{
        << ?s ?p ?o >> <{ual_pred}> ?ual
        FILTER(STRSTARTS(STR(?ual), "{ual}/"))
    }}
}}"#,
            unified = named_graphs::UNIFIED,
            ual_pred = predicates::UAL,
        );

        self.backend.ask(&query, self.config.timeouts.ask_ms).await
    }

    // ========== Knowledge Asset Operations ==========

    /// Get knowledge asset from unified graph
    ///
    /// Returns N-Quads string with triples matching the exact UAL
    pub async fn get_knowledge_asset(&self, ual: &str, visibility: Visibility) -> Result<String> {
        let filter = match visibility {
            Visibility::Public => format!(
                r#"FILTER NOT EXISTS {{ << ?s ?p ?o >> <{label}> "private" . }}"#,
                label = predicates::LABEL
            ),
            Visibility::Private | Visibility::All => String::new(),
        };

        let query = format!(
            r#"CONSTRUCT {{ ?s ?p ?o . }}
WHERE {{
    GRAPH <{unified}> {{
        << ?s ?p ?o >> <{ual_pred}> <{ual}> .
        {filter}
    }}
}}"#,
            unified = named_graphs::UNIFIED,
            ual_pred = predicates::UAL,
        );

        self.backend
            .construct(&query, self.config.timeouts.query_ms)
            .await
    }

    /// Get knowledge asset from named graph
    pub async fn get_knowledge_asset_named_graph(
        &self,
        ual: &str,
        visibility: Visibility,
    ) -> Result<String> {
        let graph = match visibility.as_suffix() {
            Some(suffix) => format!("{ual}/{suffix}"),
            None => {
                // For "all", union both graphs
                let query = format!(
                    r#"CONSTRUCT {{ ?s ?p ?o . }}
WHERE {{
    {{ GRAPH <{ual}/public> {{ ?s ?p ?o . }} }}
    UNION
    {{ GRAPH <{ual}/private> {{ ?s ?p ?o . }} }}
}}"#
                );
                return self
                    .backend
                    .construct(&query, self.config.timeouts.query_ms)
                    .await;
            }
        };

        let query = format!(
            r#"CONSTRUCT {{ ?s ?p ?o . }}
WHERE {{
    GRAPH <{graph}> {{ ?s ?p ?o . }}
}}"#
        );

        self.backend
            .construct(&query, self.config.timeouts.query_ms)
            .await
    }

    /// Delete knowledge asset from unified graph
    pub async fn delete_knowledge_asset(&self, ual: &str) -> Result<()> {
        let query = format!(
            r#"DELETE {{
    GRAPH <{unified}> {{
        ?s ?p ?o .
        << ?s ?p ?o >> ?annotationPredicate ?annotationValue .
    }}
}}
WHERE {{
    GRAPH <{unified}> {{
        << ?s ?p ?o >> <{ual_pred}> <{ual}> .
    }}
    {{
        SELECT ?s ?p ?o (COUNT(?annotationValue) AS ?annotationCount)
        WHERE {{
            GRAPH <{unified}> {{
                << ?s ?p ?o >> <{ual_pred}> ?annotationValue .
            }}
        }}
        GROUP BY ?s ?p ?o
        HAVING(?annotationCount = 1)
    }}
}}"#,
            unified = named_graphs::UNIFIED,
            ual_pred = predicates::UAL,
        );

        self.backend
            .update(&query, self.config.timeouts.insert_ms)
            .await?;

        tracing::debug!(ual = %ual, "Deleted knowledge asset from unified graph");
        Ok(())
    }

    /// Check if knowledge asset exists in unified graph
    pub async fn knowledge_asset_exists(&self, ual: &str) -> Result<bool> {
        let query = format!(
            r#"ASK WHERE {{
    GRAPH <{unified}> {{
        << ?s ?p ?o >> <{ual_pred}> <{ual}>
    }}
}}"#,
            unified = named_graphs::UNIFIED,
            ual_pred = predicates::UAL,
        );

        self.backend.ask(&query, self.config.timeouts.ask_ms).await
    }

    // ========== Metadata Operations ==========

    /// Get metadata for a UAL from the metadata graph
    pub async fn get_metadata(&self, ual: &str) -> Result<String> {
        let query = format!(
            r#"CONSTRUCT {{ <{ual}> ?p ?o . }}
WHERE {{
    GRAPH <{metadata}> {{
        <{ual}> ?p ?o .
    }}
}}"#,
            metadata = named_graphs::METADATA,
        );

        self.backend
            .construct(&query, self.config.timeouts.query_ms)
            .await
    }

    /// Delete metadata for a UAL
    pub async fn delete_metadata(&self, ual: &str) -> Result<()> {
        let query = format!(
            r#"DELETE WHERE {{
    GRAPH <{metadata}> {{
        <{ual}> ?p ?o .
    }}
}}"#,
            metadata = named_graphs::METADATA,
        );

        self.backend
            .update(&query, self.config.timeouts.insert_ms)
            .await?;

        tracing::debug!(ual = %ual, "Deleted metadata");
        Ok(())
    }

    /// Check if metadata exists for a knowledge collection
    pub async fn metadata_exists(&self, ual: &str) -> Result<bool> {
        let query = format!(
            r#"ASK {{
    GRAPH <{metadata}> {{
        ?ual ?p ?o
        FILTER(STRSTARTS(STR(?ual), "{ual}/"))
    }}
}}"#,
            metadata = named_graphs::METADATA,
        );

        self.backend.ask(&query, self.config.timeouts.ask_ms).await
    }

    // ========== Named Graph Operations ==========

    /// Drop a named graph
    pub async fn drop_named_graph(&self, graph: &str) -> Result<()> {
        let query = format!("DROP GRAPH <{graph}>");

        self.backend
            .update(&query, self.config.timeouts.insert_ms)
            .await?;

        tracing::debug!(graph = %graph, "Dropped named graph");
        Ok(())
    }

    /// Find all named graphs matching a UAL prefix
    pub async fn find_named_graphs_by_ual(&self, ual: &str) -> Result<Vec<String>> {
        let query = format!(
            r#"SELECT DISTINCT ?g
WHERE {{
    GRAPH <{current}> {{
        <{current}> <{has_graph}> ?g .
        FILTER(STRSTARTS(STR(?g), "{ual}"))
    }}
}}"#,
            current = named_graphs::CURRENT,
            has_graph = predicates::HAS_NAMED_GRAPH,
        );

        let result = self
            .backend
            .select(&query, self.config.timeouts.query_ms)
            .await?;

        Ok(result
            .rows
            .iter()
            .filter_map(|row| row.get_str("g").map(String::from))
            .collect())
    }

    // ========== Raw Query Access ==========

    /// Execute a raw SPARQL CONSTRUCT query
    pub async fn construct_raw(&self, query: &str, timeout_ms: Option<u64>) -> Result<String> {
        self.backend
            .construct(query, timeout_ms.unwrap_or(self.config.timeouts.query_ms))
            .await
    }

    /// Execute a raw SPARQL SELECT query
    pub async fn select_raw(&self, query: &str, timeout_ms: Option<u64>) -> Result<SelectResult> {
        self.backend
            .select(query, timeout_ms.unwrap_or(self.config.timeouts.query_ms))
            .await
    }

    /// Execute a raw SPARQL ASK query
    pub async fn ask_raw(&self, query: &str, timeout_ms: Option<u64>) -> Result<bool> {
        self.backend
            .ask(query, timeout_ms.unwrap_or(self.config.timeouts.ask_ms))
            .await
    }

    /// Execute a raw SPARQL UPDATE query
    pub async fn update_raw(&self, query: &str, timeout_ms: Option<u64>) -> Result<()> {
        self.backend
            .update(query, timeout_ms.unwrap_or(self.config.timeouts.insert_ms))
            .await
    } */
}
