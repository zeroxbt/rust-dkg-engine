use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_domain::{Assertion, ParsedUal, TokenIds, Visibility, parse_ual};
use dkg_network::PeerId;
use uuid::Uuid;

use crate::{
    application::{ParanetAccessResolution, TripleStoreAssertions, resolve_paranet_access},
    node_state::PeerRegistry,
};

#[derive(Debug, Clone)]
pub(crate) struct ServeGetInput {
    pub operation_id: Uuid,
    pub ual: String,
    pub token_ids: TokenIds,
    pub include_metadata: bool,
    pub paranet_ual: Option<String>,
    pub remote_peer_id: PeerId,
    pub local_peer_id: PeerId,
}

#[derive(Debug, Clone)]
pub(crate) enum ServeGetOutcome {
    Ack {
        assertion: Assertion,
        metadata: Option<Vec<String>>,
        effective_visibility: Visibility,
    },
    Nack {
        error_message: String,
    },
}

pub(crate) struct ServeGetWorkflow {
    triple_store_assertions: Arc<TripleStoreAssertions>,
    peer_registry: Arc<PeerRegistry>,
    blockchain_manager: Arc<BlockchainManager>,
}

impl ServeGetWorkflow {
    pub(crate) fn new(
        triple_store_assertions: Arc<TripleStoreAssertions>,
        peer_registry: Arc<PeerRegistry>,
        blockchain_manager: Arc<BlockchainManager>,
    ) -> Self {
        Self {
            triple_store_assertions,
            peer_registry,
            blockchain_manager,
        }
    }

    pub(crate) async fn execute(&self, input: &ServeGetInput) -> ServeGetOutcome {
        let parsed_ual = match parse_ual(&input.ual) {
            Ok(parsed) => parsed,
            Err(e) => {
                tracing::warn!(
                    operation_id = %input.operation_id,
                    ual = %input.ual,
                    error = %e,
                    "Failed to parse UAL"
                );
                return ServeGetOutcome::Nack {
                    error_message: format!("Invalid UAL: {}", e),
                };
            }
        };

        let blockchain = &parsed_ual.blockchain;
        if let Some(missing_blockchain) = self
            .peer_registry
            .first_missing_shard_membership(&input.local_peer_id, [blockchain])
        {
            tracing::warn!(
                operation_id = %input.operation_id,
                local_peer_id = %input.local_peer_id,
                blockchain = %missing_blockchain,
                "Local node not found in shard"
            );
            return ServeGetOutcome::Nack {
                error_message: "Local node not in shard".to_string(),
            };
        }

        let effective_visibility = self
            .determine_visibility_for_paranet(
                &parsed_ual,
                input.paranet_ual.as_deref(),
                &input.remote_peer_id,
                &input.local_peer_id,
            )
            .await;

        tracing::debug!(
            operation_id = %input.operation_id,
            effective_visibility = ?effective_visibility,
            "Determined effective visibility for query"
        );

        let query_result = self
            .triple_store_assertions
            .query_assertion(
                &parsed_ual,
                &input.token_ids,
                effective_visibility,
                input.include_metadata,
            )
            .await;

        match query_result {
            Ok(Some(result)) if result.assertion.has_data() => {
                tracing::debug!(
                    operation_id = %input.operation_id,
                    ual = %input.ual,
                    public_count = result.assertion.public.len(),
                    has_private = result.assertion.private.is_some(),
                    has_metadata = result.metadata.is_some(),
                    "Found assertion data"
                );

                ServeGetOutcome::Ack {
                    assertion: result.assertion,
                    metadata: result.metadata,
                    effective_visibility,
                }
            }
            Ok(_) => {
                tracing::debug!(
                    operation_id = %input.operation_id,
                    ual = %input.ual,
                    "No assertion data found"
                );
                ServeGetOutcome::Nack {
                    error_message: format!("Unable to find assertion {}", input.ual),
                }
            }
            Err(e) => {
                tracing::warn!(
                    operation_id = %input.operation_id,
                    ual = %input.ual,
                    error = %e,
                    "Triple store query failed"
                );
                ServeGetOutcome::Nack {
                    error_message: format!("Triple store query failed: {}", e),
                }
            }
        }
    }

    async fn determine_visibility_for_paranet(
        &self,
        target_ual: &ParsedUal,
        paranet_ual: Option<&str>,
        remote_peer_id: &PeerId,
        local_peer_id: &PeerId,
    ) -> Visibility {
        let Some(paranet_ual) = paranet_ual else {
            return Visibility::Public;
        };

        let resolution =
            match resolve_paranet_access(&self.blockchain_manager, target_ual, paranet_ual, false)
                .await
            {
                Ok(resolution) => resolution,
                Err(error) => {
                    tracing::debug!(
                        paranet_ual = %paranet_ual,
                        error = %error,
                        "Failed to resolve paranet access, using Public visibility"
                    );
                    return Visibility::Public;
                }
            };

        match resolution {
            ParanetAccessResolution::Open { paranet_id } => {
                tracing::debug!(
                    paranet_id = %paranet_id,
                    "Paranet is not PERMISSIONED, using Public visibility"
                );
                Visibility::Public
            }
            ParanetAccessResolution::Permissioned {
                paranet_id,
                permissioned_peer_ids,
            } => {
                if permissioned_peer_ids.contains(local_peer_id)
                    && permissioned_peer_ids.contains(remote_peer_id)
                {
                    tracing::debug!(
                        paranet_id = %paranet_id,
                        local_peer = %local_peer_id,
                        remote_peer = %remote_peer_id,
                        "Both peers are permissioned, using All visibility"
                    );
                    Visibility::All
                } else {
                    tracing::debug!(
                        paranet_id = %paranet_id,
                        local_peer = %local_peer_id,
                        remote_peer = %remote_peer_id,
                        "One or both peers not permissioned, using Public visibility"
                    );
                    Visibility::Public
                }
            }
        }
    }
}
