use chrono::{DateTime, SecondsFormat, Utc};
use dkg_domain::KnowledgeCollectionMetadata;

use crate::query::predicates;

const STATES_PREDICATE: &str = "http://schema.org/states";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataAsset {
    pub ka_ual: String,
    pub has_private_graph: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MetadataTriples {
    pub kc_has_knowledge_asset: Vec<String>,
    pub kc_has_named_graph: Vec<String>,
    pub ka_states: Vec<String>,
    pub kc_core: Vec<String>,
}

impl MetadataTriples {
    /// Build metadata triples with canonical predicates and structure.
    ///
    /// This centralizes metadata construction so insert and query/reconstruction paths
    /// stay structurally identical.
    pub fn build(
        kc_ual: &str,
        assets: &[MetadataAsset],
        metadata: Option<&KnowledgeCollectionMetadata>,
    ) -> Self {
        let mut triples = Self::default();

        for asset in assets {
            triples.kc_has_knowledge_asset.push(format!(
                "<{kc_ual}> <{}> <{}> .",
                predicates::HAS_KNOWLEDGE_ASSET,
                asset.ka_ual
            ));

            triples.kc_has_named_graph.push(format!(
                "<{kc_ual}> <{}> <{}/public> .",
                predicates::HAS_NAMED_GRAPH,
                asset.ka_ual
            ));

            if asset.has_private_graph {
                triples.kc_has_named_graph.push(format!(
                    "<{kc_ual}> <{}> <{}/private> .",
                    predicates::HAS_NAMED_GRAPH,
                    asset.ka_ual
                ));
            }

            triples.ka_states.push(format!(
                "<{}> <{STATES_PREDICATE}> \"{}:0\" .",
                asset.ka_ual, asset.ka_ual
            ));
        }

        if let Some(meta) = metadata {
            triples.kc_core.push(format!(
                "<{kc_ual}> <{}> <did:dkg:publisherKey/{}> .",
                predicates::PUBLISHED_BY,
                meta.publisher_address()
            ));
            triples.kc_core.push(format!(
                "<{kc_ual}> <{}> \"{}\" .",
                predicates::PUBLISHED_AT_BLOCK,
                meta.block_number()
            ));
            triples.kc_core.push(format!(
                "<{kc_ual}> <{}> \"{}\" .",
                predicates::PUBLISH_TX,
                meta.transaction_hash()
            ));

            let block_time_iso = DateTime::from_timestamp(meta.block_timestamp() as i64, 0)
                .map(|dt| {
                    dt.with_timezone(&Utc)
                        .to_rfc3339_opts(SecondsFormat::Millis, true)
                })
                .unwrap_or_else(|| "1970-01-01T00:00:00.000Z".to_string());

            triples.kc_core.push(format!(
                "<{kc_ual}> <{}> \"{}\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .",
                predicates::BLOCK_TIME,
                block_time_iso
            ));
        }

        triples
    }

    /// Metadata triples with KC subject only.
    pub fn kc_subject_triples(&self) -> Vec<String> {
        let mut triples = Vec::with_capacity(
            self.kc_has_knowledge_asset.len() + self.kc_has_named_graph.len() + self.kc_core.len(),
        );
        triples.extend(self.kc_has_knowledge_asset.iter().cloned());
        triples.extend(self.kc_has_named_graph.iter().cloned());
        triples.extend(self.kc_core.iter().cloned());
        triples
    }

    /// All metadata triples (KC-subject + KA-subject).
    pub fn all_triples(&self) -> Vec<String> {
        let mut triples = Vec::with_capacity(
            self.kc_has_knowledge_asset.len()
                + self.kc_has_named_graph.len()
                + self.ka_states.len()
                + self.kc_core.len(),
        );
        triples.extend(self.kc_has_knowledge_asset.iter().cloned());
        triples.extend(self.kc_has_named_graph.iter().cloned());
        triples.extend(self.ka_states.iter().cloned());
        triples.extend(self.kc_core.iter().cloned());
        triples
    }
}
