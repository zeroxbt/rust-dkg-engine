use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use crate::{
    TripleStoreManager, error::Result, metrics, query::named_graphs, types::GraphVisibility,
};

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
        let started = Instant::now();
        let backend = self.backend.name();

        let query = format!(
            r#"PREFIX schema: <http://schema.org/>
                CONSTRUCT {{ ?s ?p ?o }}
                WHERE {{
                    GRAPH <{ual}/{suffix}> {{
                        ?s ?p ?o .
                    }}
                }}"#
        );

        let result = self
            .backend_construct(&query, self.config.timeouts.query_timeout())
            .await
            .map(|rdf_lines| {
                rdf_lines
                    .lines()
                    .filter(|line| !line.trim().is_empty())
                    .map(String::from)
                    .collect::<Vec<_>>()
            });

        let result_bytes = result.as_ref().map_or(0, |lines| joined_lines_bytes(lines));
        let result_triples = result.as_ref().map_or(0, |lines| lines.len());
        metrics::record_query_operation(
            backend,
            "get_knowledge_asset_named_graph",
            suffix,
            result.as_ref().err(),
            started.elapsed(),
            result_bytes,
            result_triples,
            Some(1),
            Some(1),
        );

        result
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
        let requested_uals = if end_token_id >= start_token_id {
            let total_requested = end_token_id - start_token_id + 1;
            let burned_in_range = burned_set
                .iter()
                .filter(|id| **id >= start_token_id && **id <= end_token_id)
                .count() as u64;
            total_requested.saturating_sub(burned_in_range) as usize
        } else {
            0
        };
        let requested_uals_opt = (requested_uals > 0).then_some(requested_uals);
        let asset_count_opt = requested_uals_opt.map(|value| value as u64);
        let started = Instant::now();
        let backend = self.backend.name();

        let result: Result<Vec<String>> = async {
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
        .await;

        let result_bytes = result.as_ref().map_or(0, |lines| joined_lines_bytes(lines));
        let result_triples = result.as_ref().map_or(0, |lines| lines.len());
        metrics::record_query_operation(
            backend,
            "get_knowledge_collection_named_graphs",
            suffix,
            result.as_ref().err(),
            started.elapsed(),
            result_bytes,
            result_triples,
            asset_count_opt,
            requested_uals_opt,
        );

        result
    }

    /// Check if a knowledge asset exists in the triple store.
    ///
    /// Checks for the existence of the named graph `{ka_ual}`.
    /// The ka_ual should include the visibility suffix (e.g., `did:dkg:.../1/public`).
    pub async fn knowledge_asset_exists(&self, ka_ual_with_visibility: &str) -> Result<bool> {
        let started = Instant::now();
        let backend = self.backend.name();
        let visibility = visibility_from_graph_ual(ka_ual_with_visibility);
        let query = format!(
            r#"ASK {{
                GRAPH <{ka_ual_with_visibility}> {{
                    ?s ?p ?o
                }}
            }}"#
        );

        let result = self
            .backend_ask(&query, self.config.timeouts.ask_timeout())
            .await;

        metrics::record_query_operation(
            backend,
            "knowledge_asset_exists",
            visibility,
            result.as_ref().err(),
            started.elapsed(),
            0,
            0,
            Some(1),
            Some(1),
        );

        result
    }

    /// Get metadata for a knowledge collection from the metadata graph
    ///
    /// Returns N-Triples with metadata predicates (publishedBy, publishedAtBlock, etc.)
    pub async fn get_metadata(&self, kc_ual: &str) -> Result<String> {
        let started = Instant::now();
        let backend = self.backend.name();
        let query = format!(
            r#"CONSTRUCT {{ <{kc_ual}> ?p ?o . }}
                WHERE {{
                    GRAPH <{metadata}> {{
                        <{kc_ual}> ?p ?o .
                    }}
                }}"#,
            metadata = named_graphs::METADATA,
        );

        let result = self
            .backend_construct(&query, self.config.timeouts.query_timeout())
            .await;

        let result_bytes = result.as_ref().map_or(0, |rdf| rdf.len());
        let result_triples = result.as_ref().map_or(0, |rdf| non_empty_lines_count(rdf));
        metrics::record_query_operation(
            backend,
            "get_metadata",
            "metadata",
            result.as_ref().err(),
            started.elapsed(),
            result_bytes,
            result_triples,
            Some(1),
            Some(1),
        );

        result
    }

    /// Get metadata for multiple knowledge collections in a single query.
    ///
    /// Returns a map keyed by KC UAL containing RDF triple lines for that KC.
    pub async fn get_metadata_batch(
        &self,
        kc_uals: &[String],
    ) -> Result<HashMap<String, Vec<String>>> {
        let started = Instant::now();
        let backend = self.backend.name();

        if kc_uals.is_empty() {
            return Ok(HashMap::new());
        }

        let values = kc_uals
            .iter()
            .map(|kc_ual| format!("<{kc_ual}>"))
            .collect::<Vec<_>>()
            .join(" ");

        let query = format!(
            r#"CONSTRUCT {{ ?kc ?p ?o . }}
                WHERE {{
                    GRAPH <{metadata}> {{
                        VALUES ?kc {{ {values} }}
                        ?kc ?p ?o .
                    }}
                }}"#,
            metadata = named_graphs::METADATA,
            values = values
        );

        let result = self
            .backend_construct(&query, self.config.timeouts.query_timeout())
            .await
            .map(|rdf_lines| {
                let subject_to_kc: HashMap<String, String> = kc_uals
                    .iter()
                    .map(|kc_ual| (format!("<{kc_ual}>"), kc_ual.clone()))
                    .collect();

                let mut grouped = HashMap::new();
                for line in rdf_lines.lines().filter(|line| !line.trim().is_empty()) {
                    let Some(subject) = crate::extract_subject(line) else {
                        continue;
                    };
                    let Some(kc_ual) = subject_to_kc.get(subject) else {
                        continue;
                    };
                    grouped
                        .entry(kc_ual.clone())
                        .or_insert_with(Vec::new)
                        .push(line.to_string());
                }
                grouped
            });

        let result_bytes = result
            .as_ref()
            .map_or(0, |lines| joined_lines_bytes_map(lines));
        let result_triples = result
            .as_ref()
            .map_or(0, |lines| lines.values().map(Vec::len).sum());
        let requested = kc_uals.len();
        metrics::record_query_operation(
            backend,
            "get_metadata_batch",
            "metadata",
            result.as_ref().err(),
            started.elapsed(),
            result_bytes,
            result_triples,
            Some(requested as u64),
            Some(requested),
        );

        result
    }
}

fn joined_lines_bytes(lines: &[String]) -> usize {
    if lines.is_empty() {
        return 0;
    }
    lines.iter().map(String::len).sum::<usize>() + lines.len().saturating_sub(1)
}

fn non_empty_lines_count(text: &str) -> usize {
    text.lines().filter(|line| !line.trim().is_empty()).count()
}

fn joined_lines_bytes_map(lines_by_kc: &HashMap<String, Vec<String>>) -> usize {
    lines_by_kc
        .values()
        .map(|lines| joined_lines_bytes(lines))
        .sum()
}

fn visibility_from_graph_ual(ual: &str) -> &'static str {
    match ual.rsplit('/').next() {
        Some("public") => "public",
        Some("private") => "private",
        _ => "n/a",
    }
}
