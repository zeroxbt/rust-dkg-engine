use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use dkg_domain::{Assertion, ParsedUal, TokenIds, Visibility, parse_ual};
use dkg_network::PeerId;
use uuid::Uuid;

use crate::{application::TripleStoreAssertions, node_state::PeerRegistry};

#[derive(Debug, Clone)]
pub(crate) struct ServeBatchGetInput {
    pub operation_id: Uuid,
    pub uals: Vec<String>,
    pub token_ids: HashMap<String, TokenIds>,
    pub include_metadata: bool,
    pub local_peer_id: PeerId,
}

#[derive(Debug, Clone)]
pub(crate) enum ServeBatchGetOutcome {
    Ack {
        assertions: HashMap<String, Assertion>,
        metadata: HashMap<String, Vec<String>>,
    },
    Nack {
        error_message: String,
    },
}

pub(crate) struct ServeBatchGetWorkflow {
    triple_store_assertions: Arc<TripleStoreAssertions>,
    peer_registry: Arc<PeerRegistry>,
}

impl ServeBatchGetWorkflow {
    pub(crate) fn new(
        triple_store_assertions: Arc<TripleStoreAssertions>,
        peer_registry: Arc<PeerRegistry>,
    ) -> Self {
        Self {
            triple_store_assertions,
            peer_registry,
        }
    }

    pub(crate) async fn execute(&self, input: &ServeBatchGetInput) -> ServeBatchGetOutcome {
        let uals: Vec<String> = input
            .uals
            .iter()
            .take(crate::application::UAL_MAX_LIMIT)
            .cloned()
            .collect();

        let mut uals_with_token_ids: Vec<(ParsedUal, TokenIds)> = Vec::new();
        let mut blockchains = HashSet::new();

        for ual in &uals {
            let parsed_ual = match parse_ual(ual) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(
                        operation_id = %input.operation_id,
                        ual = %ual,
                        error = %e,
                        "Failed to parse UAL, skipping"
                    );
                    continue;
                }
            };

            let token_ids = input
                .token_ids
                .get(ual)
                .cloned()
                .unwrap_or_else(|| TokenIds::single(1));

            blockchains.insert(parsed_ual.blockchain.clone());
            uals_with_token_ids.push((parsed_ual, token_ids));
        }

        if let Some(missing_blockchain) = self
            .peer_registry
            .first_missing_shard_membership(&input.local_peer_id, blockchains.iter())
        {
            tracing::warn!(
                operation_id = %input.operation_id,
                local_peer_id = %input.local_peer_id,
                blockchain = %missing_blockchain,
                "Local node not found in shard"
            );
            return ServeBatchGetOutcome::Nack {
                error_message: "Local node not in shard".to_string(),
            };
        }

        let query_results = self
            .triple_store_assertions
            .query_assertions_batch(
                &uals_with_token_ids,
                Visibility::Public,
                input.include_metadata,
            )
            .await;

        let query_results = match query_results {
            Ok(results) => results,
            Err(e) => {
                tracing::warn!(
                    operation_id = %input.operation_id,
                    error = %e,
                    "Batch get query failed"
                );
                return ServeBatchGetOutcome::Nack {
                    error_message: format!("Triple store query failed: {}", e),
                };
            }
        };

        let mut assertions: HashMap<String, Assertion> = HashMap::new();
        let mut metadata: HashMap<String, Vec<String>> = HashMap::new();

        for (ual, result) in query_results {
            let assertion = result.assertion.clone();
            assertions.insert(ual.clone(), assertion);

            if let Some(meta) = result.metadata {
                metadata.insert(ual, meta);
            }
        }

        ServeBatchGetOutcome::Ack {
            assertions,
            metadata,
        }
    }
}
