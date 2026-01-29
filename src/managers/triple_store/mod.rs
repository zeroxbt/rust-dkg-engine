mod backend;
mod config;
pub(crate) mod error;
pub(crate) mod query;
pub(crate) mod rdf;
mod types;

use std::{path::PathBuf, sync::Arc, time::Duration};

use backend::{BlazegraphBackend, OxigraphBackend, TripleStoreBackend};
use error::{Result, TripleStoreError};
use query::{named_graphs, predicates};
use tokio::sync::Semaphore;

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
pub(crate) use config::{DKG_REPOSITORY, TripleStoreBackendType, TripleStoreManagerConfig};
pub(crate) use rdf::{extract_subject, group_nquads_by_subject, parse_metadata_from_triples};
pub(crate) use types::{Assertion, KnowledgeAsset, MAX_TOKENS_PER_KC, TokenIds, Visibility};

/// Metadata for a knowledge collection
#[derive(Debug, Clone)]
pub(crate) struct KnowledgeCollectionMetadata {
    publisher_address: String,
    block_number: u64,
    transaction_hash: String,
    block_timestamp: u64,
}

impl KnowledgeCollectionMetadata {
    pub(crate) fn new(
        publisher_address: String,
        block_number: u64,
        transaction_hash: String,
        block_timestamp: u64,
    ) -> Self {
        Self {
            publisher_address,
            block_number,
            transaction_hash,
            block_timestamp,
        }
    }

    pub(crate) fn publisher_address(&self) -> &str {
        &self.publisher_address
    }

    pub(crate) fn block_number(&self) -> u64 {
        self.block_number
    }

    pub(crate) fn transaction_hash(&self) -> &str {
        &self.transaction_hash
    }

    pub(crate) fn block_timestamp(&self) -> u64 {
        self.block_timestamp
    }
}

/// Triple Store Manager
///
/// Provides high-level operations for managing RDF data in the DKG triple store.
/// Supports knowledge collection and asset operations with SPARQL.
pub(crate) struct TripleStoreManager {
    backend: Box<dyn TripleStoreBackend>,
    config: TripleStoreManagerConfig,
    /// Semaphore for limiting concurrent operations (optional)
    concurrency_limiter: Option<Arc<Semaphore>>,
}

impl TripleStoreManager {
    /// Create a new Triple Store Manager
    ///
    /// Creates the appropriate backend based on configuration and ensures
    /// the repository/store is ready.
    pub(crate) async fn connect(config: &TripleStoreManagerConfig) -> Result<Self> {
        let backend: Box<dyn TripleStoreBackend> = match config.backend {
            TripleStoreBackendType::Blazegraph => Box::new(BlazegraphBackend::new(config.clone())?),
            TripleStoreBackendType::Oxigraph => {
                let path = config
                    .data_path
                    .clone()
                    .unwrap_or_else(|| PathBuf::from("data/triple-store"));

                // Create full path with repository name
                let store_path = path.join(DKG_REPOSITORY);

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

        // Initialize concurrency limiter if configured
        let concurrency_limiter = config.max_concurrent_operations.map(|max| {
            tracing::info!(
                max_concurrent = max,
                "Triple store concurrency limiting enabled"
            );
            Arc::new(Semaphore::new(max))
        });

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

    /// Create an in-memory Triple Store Manager (for testing)
    pub(crate) fn in_memory() -> Result<Self> {
        let backend = Box::new(OxigraphBackend::in_memory()?);
        let config = TripleStoreManagerConfig {
            backend: TripleStoreBackendType::Oxigraph,
            url: String::new(),
            data_path: None,
            username: None,
            password: None,
            connect_max_retries: 0,
            connect_retry_frequency_ms: 0,
            timeouts: Default::default(),
            max_concurrent_operations: None,
        };

        Ok(Self {
            backend,
            config,
            concurrency_limiter: None,
        })
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

    /// Health check for the triple store
    pub(crate) async fn health_check(&self) -> Result<bool> {
        self.backend.health_check().await
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

    /// Execute a SPARQL UPDATE with concurrency limiting
    async fn backend_update(&self, query: &str, timeout: Duration) -> Result<()> {
        if let Some(semaphore) = &self.concurrency_limiter {
            let _permit = semaphore.acquire().await.expect("semaphore closed");
            self.backend.update(query, timeout).await
        } else {
            self.backend.update(query, timeout).await
        }
    }

    /// Execute a SPARQL CONSTRUCT with concurrency limiting
    async fn backend_construct(&self, query: &str, timeout: Duration) -> Result<String> {
        if let Some(semaphore) = &self.concurrency_limiter {
            let _permit = semaphore.acquire().await.expect("semaphore closed");
            self.backend.construct(query, timeout).await
        } else {
            self.backend.construct(query, timeout).await
        }
    }

    /// Execute a SPARQL ASK with concurrency limiting
    async fn backend_ask(&self, query: &str, timeout: Duration) -> Result<bool> {
        if let Some(semaphore) = &self.concurrency_limiter {
            let _permit = semaphore.acquire().await.expect("semaphore closed");
            self.backend.ask(query, timeout).await
        } else {
            self.backend.ask(query, timeout).await
        }
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
    ///
    /// # Returns
    ///
    /// The total number of triples inserted (data triples + metadata triples).
    pub(crate) async fn insert_knowledge_collection(
        &self,
        kc_ual: &str,
        knowledge_assets: &[types::KnowledgeAsset],
        metadata: &Option<KnowledgeCollectionMetadata>,
    ) -> Result<usize> {
        let mut total_triples = 0;

        // Build public named graphs
        let mut public_graphs_insert = String::new();
        let mut current_public_metadata = String::new();
        let mut connection_public_metadata = String::new();

        for ka in knowledge_assets {
            let graph_uri = format!("{}/public", ka.ual());

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
            let publish_time_iso = format_unix_timestamp(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
            );
            metadata_triples.push_str(&format!(
                "    <{}> <{}> \"{}\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n",
                kc_ual,
                predicates::PUBLISH_TIME,
                publish_time_iso
            ));

            // Block time
            let block_time_iso = format_unix_timestamp(meta.block_timestamp());
            metadata_triples.push_str(&format!(
                "    <{}> <{}> \"{}\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n",
                kc_ual,
                predicates::BLOCK_TIME,
                block_time_iso
            ));
        }

        // Build the final INSERT DATA query
        let insert_query = format!(
            r#"PREFIX schema: <http://schema.org/>
                INSERT DATA {{
                {}{}  GRAPH <{}> {{
                {}{}  }}
                GRAPH <{}> {{
                {}{}{}  }}
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
        );

        tracing::debug!(
            kc_ual = %kc_ual,
            query_length = insert_query.len(),
            "Built INSERT DATA query for knowledge collection"
        );

        self.backend_update(&insert_query, self.config.timeouts.insert_timeout())
            .await?;

        tracing::debug!(
            kc_ual = %kc_ual,
            total_triples = total_triples,
            "Inserted knowledge collection"
        );

        Ok(total_triples)
    }

    /// Execute a raw SPARQL UPDATE query
    pub(crate) async fn update_raw(&self, query: &str, timeout: Option<Duration>) -> Result<()> {
        self.backend_update(
            query,
            timeout.unwrap_or_else(|| self.config.timeouts.insert_timeout()),
        )
        .await
    }

    // ========== Knowledge Asset Operations ==========

    /// Get knowledge asset from unified graph
    ///
    /// Returns N-Quads string with triples matching the exact UAL
    pub(crate) async fn get_knowledge_asset(
        &self,
        ual: &str,
        visibility: Visibility,
    ) -> Result<String> {
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

        self.backend_construct(&query, self.config.timeouts.query_timeout())
            .await
    }

    /// Get knowledge asset from its named graph (public or private).
    ///
    /// This queries the actual named graph `{ual}/public` or `{ual}/private`.
    /// For visibility ALL, call this method twice with Public and Private.
    ///
    /// Returns N-Triples as lines.
    pub(crate) async fn get_knowledge_asset_named_graph(
        &self,
        ual: &str,
        visibility: Visibility,
    ) -> Result<Vec<String>> {
        let suffix = match visibility {
            Visibility::Public => "public",
            Visibility::Private => "private",
            Visibility::All => {
                // For ALL, we need to query both separately - caller should handle this
                return Err(TripleStoreError::Other(
                    "Use Public or Private visibility, not All".to_string(),
                ));
            }
        };

        let query = format!(
            r#"PREFIX schema: <http://schema.org/>
                CONSTRUCT {{ ?s ?p ?o }}
                WHERE {{
                    GRAPH <{ual}/{suffix}> {{
                        ?s ?p ?o .
                    }}
                }}"#
        );

        let nquads = self
            .backend_construct(&query, self.config.timeouts.query_timeout())
            .await?;

        Ok(nquads
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(String::from)
            .collect())
    }

    /// Get knowledge collection from named graphs using token ID range.
    ///
    /// Uses VALUES clause for efficient querying with pagination to handle
    /// large collections (matching JS implementation which uses MAX_TOKEN_ID_PER_GET_PAGE = 50).
    /// Returns N-Triples as lines for the specified visibility.
    pub(crate) async fn get_knowledge_collection_named_graphs(
        &self,
        kc_ual: &str,
        start_token_id: u64,
        end_token_id: u64,
        burned: &[u64],
        visibility: Visibility,
    ) -> Result<Vec<String>> {
        // Matches JS MAX_TOKEN_ID_PER_GET_PAGE constant
        const MAX_TOKEN_ID_PER_PAGE: u64 = 50;

        let suffix = match visibility {
            Visibility::Public => "public",
            Visibility::Private => "private",
            Visibility::All => {
                return Err(TripleStoreError::Other(
                    "Use Public or Private visibility, not All".to_string(),
                ));
            }
        };

        let mut all_triples = Vec::new();
        let mut page_start = start_token_id;

        // Paginate through token IDs in chunks of MAX_TOKEN_ID_PER_PAGE
        while page_start <= end_token_id {
            let page_end = (page_start + MAX_TOKEN_ID_PER_PAGE - 1).min(end_token_id);

            // Build list of named graphs for this page, excluding burned tokens
            let named_graphs: Vec<String> = (page_start..=page_end)
                .filter(|id| !burned.contains(id))
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

                let nquads = self
                    .backend_construct(&query, self.config.timeouts.query_timeout())
                    .await?;

                all_triples.extend(
                    nquads
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

    /// Check if a knowledge collection exists by checking for metadata in the metadata graph.
    ///
    /// This is a fast existence check that only requires the KC UAL (no token range needed).
    /// Returns true if any metadata triple exists for the given KC UAL.
    pub(crate) async fn knowledge_collection_exists_by_ual(&self, kc_ual: &str) -> Result<bool> {
        let query = format!(
            r#"ASK {{
                GRAPH <{metadata}> {{
                    <{kc_ual}> ?p ?o
                }}
            }}"#,
            metadata = named_graphs::METADATA,
        );

        self.backend_ask(&query, self.config.timeouts.ask_timeout())
            .await
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

    /*  // ========== Knowledge Collection Operations (OLD) ==========

    /// Insert a knowledge collection into the triple store
    ///
    /// Inserts triples into:
    /// - Named graphs for public/private assertions
    /// - Metadata graph with KC metadata
    /// - Current graph with named graph references
    pub(crate) async fn   insert_knowledge_collection(
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
    pub(crate) async fn   get_knowledge_collection(
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

        self.backend_construct(&query, self.config.timeouts.query_timeout())
            .await
    } */

    /*  /// Get knowledge collection from named graphs
    ///
    /// Returns triples from the specific public/private named graphs
    pub(crate) async fn   get_knowledge_collection_named_graphs(
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

        self.backend_construct(&query, self.config.timeouts.query_timeout())
            .await
    }

    /// Delete knowledge collection from unified graph
    ///
    /// Uses annotation count to only delete triples that belong exclusively
    /// to this knowledge collection (not shared with others)
    pub(crate) async fn   delete_knowledge_collection(&self, ual: &str) -> Result<()> {
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
    pub(crate) async fn   knowledge_collection_exists(&self, ual: &str) -> Result<bool> {
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

        self.backend_ask(&query, self.config.timeouts.ask_timeout()).await
    }

    // ========== Knowledge Asset Operations ==========

    /// Get knowledge asset from unified graph
    ///
    /// Returns N-Quads string with triples matching the exact UAL
    pub(crate) async fn   get_knowledge_asset(&self, ual: &str, visibility: Visibility) -> Result<String> {
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

        self.backend_construct(&query, self.config.timeouts.query_timeout())
            .await
    }

    /// Get knowledge asset from named graph
    pub(crate) async fn   get_knowledge_asset_named_graph(
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
                    .construct(&query, self.config.timeouts.query_timeout())
                    .await;
            }
        };

        let query = format!(
            r#"CONSTRUCT {{ ?s ?p ?o . }}
WHERE {{
    GRAPH <{graph}> {{ ?s ?p ?o . }}
}}"#
        );

        self.backend_construct(&query, self.config.timeouts.query_timeout())
            .await
    }

    /// Delete knowledge asset from unified graph
    pub(crate) async fn   delete_knowledge_asset(&self, ual: &str) -> Result<()> {
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
    pub(crate) async fn   knowledge_asset_exists(&self, ual: &str) -> Result<bool> {
        let query = format!(
            r#"ASK WHERE {{
    GRAPH <{unified}> {{
        << ?s ?p ?o >> <{ual_pred}> <{ual}>
    }}
}}"#,
            unified = named_graphs::UNIFIED,
            ual_pred = predicates::UAL,
        );

        self.backend_ask(&query, self.config.timeouts.ask_timeout()).await
    }

    // ========== Metadata Operations ==========

    /// Get metadata for a UAL from the metadata graph
    pub(crate) async fn   get_metadata(&self, ual: &str) -> Result<String> {
        let query = format!(
            r#"CONSTRUCT {{ <{ual}> ?p ?o . }}
WHERE {{
    GRAPH <{metadata}> {{
        <{ual}> ?p ?o .
    }}
}}"#,
            metadata = named_graphs::METADATA,
        );

        self.backend_construct(&query, self.config.timeouts.query_timeout())
            .await
    }

    /// Delete metadata for a UAL
    pub(crate) async fn   delete_metadata(&self, ual: &str) -> Result<()> {
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
    pub(crate) async fn   metadata_exists(&self, ual: &str) -> Result<bool> {
        let query = format!(
            r#"ASK {{
    GRAPH <{metadata}> {{
        ?ual ?p ?o
        FILTER(STRSTARTS(STR(?ual), "{ual}/"))
    }}
}}"#,
            metadata = named_graphs::METADATA,
        );

        self.backend_ask(&query, self.config.timeouts.ask_timeout()).await
    }

    // ========== Named Graph Operations ==========

    /// Drop a named graph
    pub(crate) async fn   drop_named_graph(&self, graph: &str) -> Result<()> {
        let query = format!("DROP GRAPH <{graph}>");

        self.backend
            .update(&query, self.config.timeouts.insert_ms)
            .await?;

        tracing::debug!(graph = %graph, "Dropped named graph");
        Ok(())
    }

    /// Find all named graphs matching a UAL prefix
    pub(crate) async fn   find_named_graphs_by_ual(&self, ual: &str) -> Result<Vec<String>> {
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
            .select(&query, self.config.timeouts.query_timeout())
            .await?;

        Ok(result
            .rows
            .iter()
            .filter_map(|row| row.get_str("g").map(String::from))
            .collect())
    }

    // ========== Raw Query Access ==========

    /// Execute a raw SPARQL CONSTRUCT query
    pub(crate) async fn   construct_raw(&self, query: &str, timeout_ms: Option<u64>) -> Result<String> {
        self.backend
            .construct(query, timeout_ms.unwrap_or(self.config.timeouts.query_timeout()))
            .await
    }

    /// Execute a raw SPARQL SELECT query
    pub(crate) async fn   select_raw(&self, query: &str, timeout_ms: Option<u64>) -> Result<SelectResult> {
        self.backend
            .select(query, timeout_ms.unwrap_or(self.config.timeouts.query_timeout()))
            .await
    }

    /// Execute a raw SPARQL ASK query
    pub(crate) async fn   ask_raw(&self, query: &str, timeout_ms: Option<u64>) -> Result<bool> {
        self.backend
            .ask(query, timeout_ms.unwrap_or(self.config.timeouts.ask_timeout()))
            .await
    }

    /// Execute a raw SPARQL UPDATE query
    pub(crate) async fn   update_raw(&self, query: &str, timeout_ms: Option<u64>) -> Result<()> {
        self.backend
            .update(query, timeout_ms.unwrap_or(self.config.timeouts.insert_ms))
            .await
    } */
}
