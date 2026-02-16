mod backend;
mod config;
pub(crate) mod error;
pub(crate) mod query;
pub(crate) mod rdf;
pub(crate) mod types;
use std::{collections::HashSet, path::Path, sync::Arc, time::Duration};

use backend::{BlazegraphBackend, OxigraphBackend, TripleStoreBackend};
use chrono::{DateTime, SecondsFormat, Utc};
pub(crate) use config::{
    DKG_REPOSITORY, TimeoutConfig, TripleStoreBackendType, TripleStoreManagerConfig,
};
use error::{Result, TripleStoreError};
use query::{named_graphs, predicates};
pub(crate) use rdf::{
    compare_js_default_string_order, extract_subject, group_triples_by_subject,
    parse_metadata_from_triples,
};
use serde::Deserialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
pub(crate) use types::GraphVisibility;

use dkg_domain::{KnowledgeAsset, KnowledgeCollectionMetadata};

#[cfg(test)]
mod tests;

/// Triple Store Manager
///
/// Provides high-level operations for managing RDF data in the DKG triple store.
/// Supports knowledge collection and asset operations with SPARQL.
pub(crate) struct TripleStoreManager {
    backend: Box<dyn TripleStoreBackend>,
    config: TripleStoreManagerConfig,
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
    pub(crate) async fn connect(
        config: &TripleStoreManagerConfig,
        data_path: &Path,
    ) -> Result<Self> {
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
    pub(crate) async fn ensure_repository(&self) -> Result<()> {
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
    async fn backend_update(&self, query: &str, timeout: Duration) -> Result<()> {
        let _permit = self.acquire_permit().await?;
        self.backend.update(query, timeout).await
    }

    #[cfg(test)]
    pub(crate) async fn raw_update_for_tests(&self, query: &str) -> Result<()> {
        // Convenience wrapper for tests that need to inject raw triples.
        self.backend_update(query, self.config.timeouts.insert_timeout())
            .await
    }

    /// Execute a SPARQL CONSTRUCT with concurrency limiting
    async fn backend_construct(&self, query: &str, timeout: Duration) -> Result<String> {
        let _permit = self.acquire_permit().await?;
        self.backend.construct(query, timeout).await
    }

    /// Execute a SPARQL ASK with concurrency limiting
    async fn backend_ask(&self, query: &str, timeout: Duration) -> Result<bool> {
        let _permit = self.acquire_permit().await?;
        self.backend.ask(query, timeout).await
    }

    /// Execute a SPARQL SELECT with concurrency limiting
    async fn backend_select(&self, query: &str, timeout: Duration) -> Result<String> {
        let _permit = self.acquire_permit().await?;
        self.backend.select(query, timeout).await
    }

    // ========== Knowledge Collection Operations ==========

    /// Insert a knowledge collection into the triple store.
    ///
    /// This method handles the RDF serialization and SPARQL query building.
    /// It creates:
    /// - Named graphs for each knowledge asset's public/private triples
    /// - Metadata graph entries for the knowledge collection
    /// - Current graph entries tracking named graphs
    ///
    /// # Arguments
    ///
    /// * `kc_ual` - The UAL of the knowledge collection
    /// * `knowledge_assets` - The list of knowledge assets with their triples
    /// * `metadata` - Metadata about the knowledge collection (publisher, block, tx, etc.)
    /// * `paranet_ual` - Optional paranet UAL for creating paranet graph connections
    ///
    /// # Returns
    ///
    /// The total number of triples inserted (data triples + metadata triples).
    pub(crate) async fn insert_knowledge_collection(
        &self,
        kc_ual: &str,
        knowledge_assets: &[KnowledgeAsset],
        metadata: &Option<KnowledgeCollectionMetadata>,
        paranet_ual: Option<&str>,
    ) -> Result<usize> {
        let mut total_triples = 0;
        let mut all_named_graphs: Vec<String> = Vec::new();

        // Build public named graphs
        let mut public_graphs_insert = String::new();
        let mut current_public_metadata = String::new();
        let mut connection_public_metadata = String::new();

        for ka in knowledge_assets {
            let graph_uri = format!("{}/public", ka.ual());
            all_named_graphs.push(graph_uri.clone());

            // GRAPH <ual/public> { triples }
            public_graphs_insert.push_str(&format!("  GRAPH <{}> {{\n", graph_uri));
            for triple in ka.public_triples() {
                public_graphs_insert.push_str(&format!("    {}\n", triple));
                total_triples += 1;
            }
            public_graphs_insert.push_str("  }\n");

            // current:graph hasNamedGraph <ual/public>
            current_public_metadata.push_str(&format!(
                "    <{}> <{}> <{}> .\n",
                named_graphs::CURRENT,
                predicates::HAS_NAMED_GRAPH,
                graph_uri
            ));
            total_triples += 1; // current metadata triple

            // KC hasKnowledgeAsset KA
            connection_public_metadata.push_str(&format!(
                "    <{}> <{}> <{}> .\n",
                kc_ual,
                predicates::HAS_KNOWLEDGE_ASSET,
                ka.ual()
            ));
            // KC hasNamedGraph <ual/public>
            connection_public_metadata.push_str(&format!(
                "    <{}> <{}> <{}> .\n",
                kc_ual,
                predicates::HAS_NAMED_GRAPH,
                graph_uri
            ));
        }

        // Build private named graphs
        let mut private_graphs_insert = String::new();
        let mut current_private_metadata = String::new();
        let mut connection_private_metadata = String::new();

        for ka in knowledge_assets {
            if let Some(private_triples) = ka.private_triples()
                && !private_triples.is_empty()
            {
                let graph_uri = format!("{}/private", ka.ual());
                all_named_graphs.push(graph_uri.clone());

                private_graphs_insert.push_str(&format!("  GRAPH <{}> {{\n", graph_uri));
                for triple in private_triples {
                    private_graphs_insert.push_str(&format!("    {}\n", triple));
                    total_triples += 1;
                }
                private_graphs_insert.push_str("  }\n");

                current_private_metadata.push_str(&format!(
                    "    <{}> <{}> <{}> .\n",
                    named_graphs::CURRENT,
                    predicates::HAS_NAMED_GRAPH,
                    graph_uri
                ));
                total_triples += 1; // current metadata triple

                // KC hasKnowledgeAsset KA (for private)
                connection_private_metadata.push_str(&format!(
                    "    <{}> <{}> <{}> .\n",
                    kc_ual,
                    predicates::HAS_KNOWLEDGE_ASSET,
                    ka.ual()
                ));
                // KC hasNamedGraph <ual/private>
                connection_private_metadata.push_str(&format!(
                    "    <{}> <{}> <{}> .\n",
                    kc_ual,
                    predicates::HAS_NAMED_GRAPH,
                    graph_uri
                ));
            }
        }

        // Build metadata triples
        let mut metadata_triples = String::new();

        // State triples for each KA
        for ka in knowledge_assets {
            metadata_triples.push_str(&format!(
                "    <{}> <http://schema.org/states> \"{}:0\" .\n",
                ka.ual(),
                ka.ual()
            ));
        }

        // KC metadata (only if provided)
        if let Some(meta) = metadata {
            metadata_triples.push_str(&format!(
                "    <{}> <{}> <did:dkg:publisherKey/{}> .\n",
                kc_ual,
                predicates::PUBLISHED_BY,
                meta.publisher_address()
            ));
            metadata_triples.push_str(&format!(
                "    <{}> <{}> \"{}\" .\n",
                kc_ual,
                predicates::PUBLISHED_AT_BLOCK,
                meta.block_number()
            ));
            metadata_triples.push_str(&format!(
                "    <{}> <{}> \"{}\" .\n",
                kc_ual,
                predicates::PUBLISH_TX,
                meta.transaction_hash()
            ));

            // Publish time (current time)
            let publish_time_iso = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
            metadata_triples.push_str(&format!(
                "    <{}> <{}> \"{}\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n",
                kc_ual,
                predicates::PUBLISH_TIME,
                publish_time_iso
            ));

            // Block time
            let block_time_iso = Self::format_unix_timestamp(meta.block_timestamp());
            metadata_triples.push_str(&format!(
                "    <{}> <{}> \"{}\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n",
                kc_ual,
                predicates::BLOCK_TIME,
                block_time_iso
            ));
        }

        // Optional paranet graph connections
        let mut paranet_graph_insert = String::new();
        if let Some(paranet_ual) = paranet_ual
            && !all_named_graphs.is_empty()
        {
            paranet_graph_insert.push_str(&format!("  GRAPH <{}> {{\n", paranet_ual));
            for graph_uri in &all_named_graphs {
                paranet_graph_insert.push_str(&format!(
                    "    <{}> <{}> <{}> .\n",
                    paranet_ual,
                    predicates::HAS_NAMED_GRAPH,
                    graph_uri
                ));
            }
            paranet_graph_insert.push_str("  }\n");
            total_triples += all_named_graphs.len();
        }

        // Build final SPARQL update query.
        //
        // If metadata is present, ensure publishTime is unique by deleting any prior values before
        // inserting the new publishTime. Without this, repeated inserts can accumulate multiple
        // publishTime values (since it uses Utc::now()).
        let insert_query = if metadata.is_some() {
            format!(
                r#"PREFIX schema: <http://schema.org/>
                DELETE {{
                  GRAPH <{metadata_graph}> {{
                    <{kc_ual}> <{publish_time}> ?old_publish_time .
                  }}
                }}
                INSERT {{
                {public_graphs}{private_graphs}  GRAPH <{current_graph}> {{
                {current_public}{current_private}  }}
                GRAPH <{metadata_graph}> {{
                {conn_public}{conn_private}{meta_triples}  }}
                {paranet_graph}
                }}
                WHERE {{
                  OPTIONAL {{
                    GRAPH <{metadata_graph}> {{
                      <{kc_ual}> <{publish_time}> ?old_publish_time .
                    }}
                  }}
                }}"#,
                kc_ual = kc_ual,
                publish_time = predicates::PUBLISH_TIME,
                public_graphs = public_graphs_insert,
                private_graphs = private_graphs_insert,
                current_graph = named_graphs::CURRENT,
                current_public = current_public_metadata,
                current_private = current_private_metadata,
                metadata_graph = named_graphs::METADATA,
                conn_public = connection_public_metadata,
                conn_private = connection_private_metadata,
                meta_triples = metadata_triples,
                paranet_graph = paranet_graph_insert,
            )
        } else {
            // No publishTime is inserted when metadata is absent; keep a simpler INSERT DATA.
            format!(
                r#"PREFIX schema: <http://schema.org/>
                INSERT DATA {{
                {}{}  GRAPH <{}> {{
                {}{}  }}
                GRAPH <{}> {{
                {}{}{}  }}
                {}
                }}"#,
                public_graphs_insert,
                private_graphs_insert,
                named_graphs::CURRENT,
                current_public_metadata,
                current_private_metadata,
                named_graphs::METADATA,
                connection_public_metadata,
                connection_private_metadata,
                metadata_triples,
                paranet_graph_insert,
            )
        };

        tracing::trace!(
            kc_ual = %kc_ual,
            query_length = insert_query.len(),
            "Built INSERT DATA query for knowledge collection"
        );

        self.backend_update(&insert_query, self.config.timeouts.insert_timeout())
            .await?;

        tracing::trace!(
            kc_ual = %kc_ual,
            total_triples = total_triples,
            "Inserted knowledge collection"
        );

        Ok(total_triples)
    }
    // ========== Knowledge Asset Operations ==========

    /// Get knowledge asset from its named graph (public or private).
    ///
    /// This queries the actual named graph `{ual}/public` or `{ual}/private`.
    ///
    /// Returns RDF lines (N-Triples/N-Quads).
    pub(crate) async fn get_knowledge_asset_named_graph(
        &self,
        ual: &str,
        visibility: GraphVisibility,
    ) -> Result<Vec<String>> {
        let suffix = visibility.as_suffix();

        let query = format!(
            r#"PREFIX schema: <http://schema.org/>
                CONSTRUCT {{ ?s ?p ?o }}
                WHERE {{
                    GRAPH <{ual}/{suffix}> {{
                        ?s ?p ?o .
                    }}
                }}"#
        );

        let rdf_lines = self
            .backend_construct(&query, self.config.timeouts.query_timeout())
            .await?;

        Ok(rdf_lines
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(String::from)
            .collect())
    }

    /// Get knowledge collection from named graphs using token ID range.
    ///
    /// Uses VALUES clause for efficient querying with pagination to handle
    /// large collections (matching JS implementation which uses MAX_TOKEN_ID_PER_GET_PAGE = 50).
    /// Returns RDF lines (N-Triples/N-Quads) for the specified visibility.
    pub(crate) async fn get_knowledge_collection_named_graphs(
        &self,
        kc_ual: &str,
        start_token_id: u64,
        end_token_id: u64,
        burned: &[u64],
        visibility: GraphVisibility,
    ) -> Result<Vec<String>> {
        // Matches JS MAX_TOKEN_ID_PER_GET_PAGE constant
        const MAX_TOKEN_ID_PER_PAGE: u64 = 50;

        let suffix = visibility.as_suffix();
        let burned_set: std::collections::HashSet<u64> = burned.iter().copied().collect();

        let mut all_triples = Vec::new();
        let mut page_start = start_token_id;

        // Paginate through token IDs in chunks of MAX_TOKEN_ID_PER_PAGE
        while page_start <= end_token_id {
            let page_end = (page_start + MAX_TOKEN_ID_PER_PAGE - 1).min(end_token_id);

            // Build list of named graphs for this page, excluding burned tokens
            let named_graphs: Vec<String> = (page_start..=page_end)
                .filter(|id| !burned_set.contains(id))
                .map(|id| format!("<{}/{}/{}>", kc_ual, id, suffix))
                .collect();

            if !named_graphs.is_empty() {
                // Use VALUES clause like JS implementation
                let query = format!(
                    r#"PREFIX schema: <http://schema.org/>
                        CONSTRUCT {{
                            ?s ?p ?o .
                        }}
                        WHERE {{
                            GRAPH ?g {{
                                ?s ?p ?o .
                            }}
                            VALUES ?g {{
                                {}
                            }}
                        }}"#,
                    named_graphs.join("\n        ")
                );

                let rdf_lines = self
                    .backend_construct(&query, self.config.timeouts.query_timeout())
                    .await?;

                all_triples.extend(
                    rdf_lines
                        .lines()
                        .filter(|line| !line.trim().is_empty())
                        .map(String::from),
                );
            }

            page_start = page_end + 1;
        }

        Ok(all_triples)
    }

    /// Check if a knowledge asset exists in the triple store.
    ///
    /// Checks for the existence of the named graph `{ka_ual}`.
    /// The ka_ual should include the visibility suffix (e.g., `did:dkg:.../1/public`).
    pub(crate) async fn knowledge_asset_exists(
        &self,
        ka_ual_with_visibility: &str,
    ) -> Result<bool> {
        let query = format!(
            r#"ASK {{
                GRAPH <{ka_ual_with_visibility}> {{
                    ?s ?p ?o
                }}
            }}"#
        );

        self.backend_ask(&query, self.config.timeouts.ask_timeout())
            .await
    }

    /// Check which knowledge collections exist by UAL (batched).
    ///
    /// Returns the subset of UALs that exist in the metadata graph.
    pub(crate) async fn knowledge_collections_exist_by_uals(
        &self,
        kc_uals: &[String],
    ) -> Result<HashSet<String>> {
        const BATCH_SIZE: usize = 200;
        let mut existing = HashSet::new();

        if kc_uals.is_empty() {
            return Ok(existing);
        }

        for chunk in kc_uals.chunks(BATCH_SIZE) {
            let values: String = chunk
                .iter()
                .map(|ual| format!("<{}>", ual))
                .collect::<Vec<_>>()
                .join(" ");

            let query = format!(
                r#"SELECT ?kc WHERE {{
                    GRAPH <{metadata}> {{
                        VALUES ?kc {{ {values} }}
                        ?kc ?p ?o
                    }}
                }}"#,
                metadata = named_graphs::METADATA,
                values = values
            );

            let response = self
                .backend_select(&query, self.config.timeouts.query_timeout())
                .await?;

            let selected = parse_select_values(&response, "kc")?;
            existing.extend(selected);
        }

        Ok(existing)
    }

    /// Get metadata for a knowledge collection from the metadata graph
    ///
    /// Returns N-Triples with metadata predicates (publishedBy, publishedAtBlock, etc.)
    pub(crate) async fn get_metadata(&self, kc_ual: &str) -> Result<String> {
        let query = format!(
            r#"CONSTRUCT {{ <{kc_ual}> ?p ?o . }}
                WHERE {{
                    GRAPH <{metadata}> {{
                        <{kc_ual}> ?p ?o .
                    }}
                }}"#,
            metadata = named_graphs::METADATA,
        );

        self.backend_construct(&query, self.config.timeouts.query_timeout())
            .await
    }

    /// Format a unix timestamp as ISO 8601 datetime string
    fn format_unix_timestamp(timestamp: u64) -> String {
        DateTime::from_timestamp(timestamp as i64, 0)
            .unwrap_or(DateTime::UNIX_EPOCH)
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string()
    }
}

#[cfg(test)]
impl TripleStoreManager {
    pub(crate) fn from_backend_for_tests(
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

// ========== SPARQL SELECT Parsing ==========

#[derive(Deserialize)]
struct SparqlSelectResponse {
    results: SparqlSelectResults,
}

#[derive(Deserialize)]
struct SparqlSelectResults {
    bindings: Vec<std::collections::HashMap<String, SparqlSelectBinding>>,
}

#[derive(Deserialize)]
struct SparqlSelectBinding {
    value: String,
}

fn parse_select_values(json: &str, var: &str) -> Result<HashSet<String>> {
    let response: SparqlSelectResponse =
        serde_json::from_str(json).map_err(|e| TripleStoreError::ParseError {
            reason: format!("Failed to parse SELECT response: {e}"),
        })?;

    let mut values = HashSet::new();
    for binding in response.results.bindings {
        if let Some(value) = binding.get(var) {
            values.insert(value.value.clone());
        }
    }

    Ok(values)
}
