use std::collections::HashSet;

use crate::TripleStoreManager;
use crate::error::Result;
use crate::query::named_graphs;
use crate::types::GraphVisibility;

impl TripleStoreManager {
    /// Get knowledge asset from its named graph (public or private).
    ///
    /// This queries the actual named graph `{ual}/public` or `{ual}/private`.
    ///
    /// Returns RDF lines (N-Triples/N-Quads).
    pub async fn get_knowledge_asset_named_graph(
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
    pub async fn get_knowledge_collection_named_graphs(
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
        let burned_set: HashSet<u64> = burned.iter().copied().collect();

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
    pub async fn knowledge_asset_exists(&self, ka_ual_with_visibility: &str) -> Result<bool> {
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

    /// Get metadata for a knowledge collection from the metadata graph
    ///
    /// Returns N-Triples with metadata predicates (publishedBy, publishedAtBlock, etc.)
    pub async fn get_metadata(&self, kc_ual: &str) -> Result<String> {
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
}
