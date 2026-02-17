use chrono::{DateTime, SecondsFormat, Utc};
use dkg_domain::{KnowledgeAsset, KnowledgeCollectionMetadata};

use crate::{
    TripleStoreManager,
    error::Result,
    query::{named_graphs, predicates},
};

impl TripleStoreManager {
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
    pub async fn insert_knowledge_collection(
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

    /// Check which knowledge collections exist by UAL (batched).
    ///
    /// Returns the subset of UALs that exist in the metadata graph.
    pub async fn knowledge_collections_exist_by_uals(
        &self,
        kc_uals: &[String],
    ) -> Result<std::collections::HashSet<String>> {
        const BATCH_SIZE: usize = 200;
        let mut existing = std::collections::HashSet::new();

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

            let selected = crate::sparql::parse_select_values(&response, "kc")?;
            existing.extend(selected);
        }

        Ok(existing)
    }

    /// Format a unix timestamp as ISO 8601 datetime string
    fn format_unix_timestamp(timestamp: u64) -> String {
        DateTime::from_timestamp(timestamp as i64, 0)
            .unwrap_or(DateTime::UNIX_EPOCH)
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string()
    }
}
