use std::time::Instant;

use dkg_domain::{KnowledgeAsset, KnowledgeCollectionMetadata};

use crate::{
    MetadataAsset, MetadataTriples, TripleStoreManager,
    error::Result,
    metrics::{self, KcInsertCharacteristics},
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
        let started = Instant::now();
        let backend = self.backend.name();
        let insert_characteristics = KcInsertCharacteristics::from_assets(
            knowledge_assets,
            metadata.is_some(),
            paranet_ual.is_some(),
        );

        let mut total_triples = 0;
        let track_paranet_graphs = paranet_ual.is_some();
        let mut all_named_graphs: Vec<String> = Vec::new();

        let metadata_assets: Vec<MetadataAsset> = knowledge_assets
            .iter()
            .map(|ka| MetadataAsset {
                ka_ual: ka.ual().to_string(),
                has_private_graph: ka
                    .private_triples()
                    .is_some_and(|triples| !triples.is_empty()),
            })
            .collect();
        let built_metadata = MetadataTriples::build(kc_ual, &metadata_assets, metadata.as_ref());

        // Build public named graphs
        let mut public_graphs_insert = String::new();
        let mut current_public_metadata = String::new();

        for ka in knowledge_assets {
            let graph_uri = format!("{}/public", ka.ual());
            if track_paranet_graphs {
                all_named_graphs.push(graph_uri.clone());
            }

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
        }

        // Build private named graphs
        let mut private_graphs_insert = String::new();
        let mut current_private_metadata = String::new();

        for ka in knowledge_assets {
            if let Some(private_triples) = ka.private_triples()
                && !private_triples.is_empty()
            {
                let graph_uri = format!("{}/private", ka.ual());
                if track_paranet_graphs {
                    all_named_graphs.push(graph_uri.clone());
                }

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
            }
        }

        let kc_to_ka_metadata = built_metadata
            .kc_has_knowledge_asset
            .iter()
            .map(|line| format!("    {line}\n"))
            .collect::<String>();
        let kc_named_graph_metadata = built_metadata
            .kc_has_named_graph
            .iter()
            .map(|line| format!("    {line}\n"))
            .collect::<String>();
        let ka_state_metadata = built_metadata
            .ka_states
            .iter()
            .map(|line| format!("    {line}\n"))
            .collect::<String>();
        let kc_core_metadata = built_metadata
            .kc_core
            .iter()
            .map(|line| format!("    {line}\n"))
            .collect::<String>();

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

        let insert_query = format!(
            r#"PREFIX schema: <http://schema.org/>
                INSERT DATA {{
                {}{}  GRAPH <{}> {{
                {}{}  }}
                GRAPH <{}> {{
                {}{}{}{}  }}
                {}
                }}"#,
            public_graphs_insert,
            private_graphs_insert,
            named_graphs::CURRENT,
            current_public_metadata,
            current_private_metadata,
            named_graphs::METADATA,
            kc_to_ka_metadata,
            kc_named_graph_metadata,
            ka_state_metadata,
            kc_core_metadata,
            paranet_graph_insert,
        );

        tracing::trace!(
            kc_ual = %kc_ual,
            query_length = insert_query.len(),
            "Built INSERT DATA query for knowledge collection"
        );

        let insert_result = self
            .backend_update(&insert_query, self.config.timeouts.insert_timeout())
            .await;

        metrics::record_kc_insert(
            backend,
            &insert_characteristics,
            total_triples,
            insert_result.as_ref().err(),
            started.elapsed(),
        );

        insert_result?;

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
}
