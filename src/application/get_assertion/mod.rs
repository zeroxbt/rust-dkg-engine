pub(crate) mod config;
pub(crate) mod network_fetch;

use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_domain::{
    Assertion, BlockchainId, ParsedUal, TokenIds, UalParseError, Visibility, parse_ual,
};
use dkg_network::{GetRequestData, NetworkManager, PeerId, STREAM_PROTOCOL_GET};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    application::{AssertionValidation, TripleStoreAssertions, paranet},
    node_state::PeerRegistry,
};

#[derive(Debug, Clone, Copy)]
pub(crate) enum AssertionSource {
    Local,
    Network,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum TokenRangeResolutionPolicy {
    Strict,
    CompatibleSingleTokenFallback,
}

#[derive(Debug, Error)]
pub(crate) enum GetAssertionError {
    #[error("Invalid UAL: {0}")]
    InvalidUal(#[from] UalParseError),
    #[error(
        "Knowledge collection {knowledge_collection_id} does not exist on blockchain {blockchain}"
    )]
    MissingKnowledgeCollection {
        blockchain: BlockchainId,
        knowledge_collection_id: u128,
    },
    #[error(
        "Unable to find enough nodes for operation: {operation_id}. Found 0 nodes, need at least 1"
    )]
    NotEnoughPeers { operation_id: Uuid },
    #[error("Failed to get data from network for operation: {operation_id}")]
    NetworkFetchFailed { operation_id: Uuid },
    #[error("Failed to resolve token range for operation {operation_id}: {reason}")]
    TokenRangeResolution { operation_id: Uuid, reason: String },
    #[error("{0}")]
    ShardPeerSelection(String),
}

#[derive(Debug, Clone)]
pub(crate) struct GetAssertionInput {
    pub operation_id: Uuid,
    pub ual: String,
    pub include_metadata: bool,
    pub paranet_ual: Option<String>,
    pub visibility: Visibility,
}

#[derive(Debug, Clone)]
pub(crate) struct GetAssertionOutput {
    pub assertion: Assertion,
    pub metadata: Option<Vec<String>>,
    pub source: AssertionSource,
}

pub(crate) struct GetAssertionUseCase {
    blockchain_manager: Arc<BlockchainManager>,
    triple_store_assertions: Arc<TripleStoreAssertions>,
    network_manager: Arc<NetworkManager>,
    assertion_validation: Arc<AssertionValidation>,
    peer_registry: Arc<PeerRegistry>,
}

impl GetAssertionUseCase {
    pub(crate) fn new(
        blockchain_manager: Arc<BlockchainManager>,
        triple_store_assertions: Arc<TripleStoreAssertions>,
        network_manager: Arc<NetworkManager>,
        assertion_validation: Arc<AssertionValidation>,
        peer_registry: Arc<PeerRegistry>,
    ) -> Self {
        Self {
            blockchain_manager,
            triple_store_assertions,
            network_manager,
            assertion_validation,
            peer_registry,
        }
    }

    pub(crate) async fn fetch(
        &self,
        request: &GetAssertionInput,
    ) -> Result<GetAssertionOutput, GetAssertionError> {
        let parsed_ual = parse_ual(&request.ual)?;

        // Validate on-chain existence when possible. If call fails, continue (old contracts may
        // not support all paths, matching existing behavior).
        match self
            .blockchain_manager
            .get_knowledge_collection_publisher(
                &parsed_ual.blockchain,
                parsed_ual.contract,
                parsed_ual.knowledge_collection_id,
            )
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Err(GetAssertionError::MissingKnowledgeCollection {
                    blockchain: parsed_ual.blockchain.clone(),
                    knowledge_collection_id: parsed_ual.knowledge_collection_id,
                });
            }
            Err(e) => {
                tracing::warn!(
                    operation_id = %request.operation_id,
                    error = %e,
                    "Failed to validate UAL on-chain, continuing"
                );
            }
        }

        let token_ids = self
            .resolve_token_ids_with_policy(
                request.operation_id,
                &parsed_ual,
                TokenRangeResolutionPolicy::CompatibleSingleTokenFallback,
            )
            .await?;

        if let Some(local) = self
            .try_local(
                request.operation_id,
                &parsed_ual,
                &token_ids,
                request.visibility,
                request.include_metadata,
            )
            .await
        {
            return Ok(local);
        }

        self.try_network(request.operation_id, &parsed_ual, token_ids, request)
            .await
    }

    pub(crate) async fn resolve_token_ids_with_policy(
        &self,
        operation_id: Uuid,
        parsed_ual: &ParsedUal,
        policy: TokenRangeResolutionPolicy,
    ) -> Result<TokenIds, GetAssertionError> {
        resolve_token_ids(
            self.blockchain_manager.as_ref(),
            operation_id,
            parsed_ual,
            policy,
        )
        .await
    }

    async fn try_local(
        &self,
        operation_id: Uuid,
        parsed_ual: &ParsedUal,
        token_ids: &TokenIds,
        visibility: Visibility,
        include_metadata: bool,
    ) -> Option<GetAssertionOutput> {
        let local_result = self
            .triple_store_assertions
            .query_assertion(parsed_ual, token_ids, visibility, include_metadata)
            .await
            .ok()?;

        let result = local_result?;
        if !result.assertion.has_data() {
            return None;
        }

        let is_valid = self
            .assertion_validation
            .validate_response(&result.assertion, parsed_ual, visibility)
            .await;

        if !is_valid {
            tracing::debug!(
                operation_id = %operation_id,
                "Local assertion validation failed"
            );
            return None;
        }

        Some(GetAssertionOutput {
            assertion: Assertion::new(
                result.assertion.public.clone(),
                result.assertion.private.clone(),
            ),
            metadata: result.metadata.clone(),
            source: AssertionSource::Local,
        })
    }

    async fn try_network(
        &self,
        operation_id: Uuid,
        parsed_ual: &ParsedUal,
        token_ids: TokenIds,
        request: &GetAssertionInput,
    ) -> Result<GetAssertionOutput, GetAssertionError> {
        let mut peers = self
            .load_shard_peers(parsed_ual, request.paranet_ual.as_deref())
            .await?;

        if peers.is_empty() {
            return Err(GetAssertionError::NotEnoughPeers { operation_id });
        }

        self.peer_registry.sort_by_latency(&mut peers);

        let get_request_data = GetRequestData::new(
            parsed_ual.blockchain.clone(),
            format!("{:?}", parsed_ual.contract),
            parsed_ual.knowledge_collection_id,
            parsed_ual.knowledge_asset_id,
            request.ual.clone(),
            token_ids,
            request.include_metadata,
            request.paranet_ual.clone(),
        );

        if let Some(ack) = network_fetch::fetch_first_valid_ack_from_peers(
            Arc::clone(&self.network_manager),
            Arc::clone(&self.assertion_validation),
            peers,
            operation_id,
            get_request_data,
            parsed_ual.clone(),
            request.visibility,
        )
        .await
        {
            return Ok(GetAssertionOutput {
                assertion: Assertion::new(
                    ack.assertion.public.clone(),
                    ack.assertion.private.clone(),
                ),
                metadata: ack.metadata.clone(),
                source: AssertionSource::Network,
            });
        }

        Err(GetAssertionError::NetworkFetchFailed { operation_id })
    }

    async fn load_shard_peers(
        &self,
        parsed_ual: &ParsedUal,
        paranet_ual: Option<&str>,
    ) -> Result<Vec<PeerId>, GetAssertionError> {
        let my_peer_id = self.network_manager.peer_id();
        let all_shard_peers = self.peer_registry.select_shard_peers(
            &parsed_ual.blockchain,
            STREAM_PROTOCOL_GET,
            Some(my_peer_id),
        );

        if let Some(paranet_ual) = paranet_ual {
            paranet::select_shard_peers_for_paranet(
                self.blockchain_manager.as_ref(),
                parsed_ual,
                paranet_ual,
                all_shard_peers,
            )
            .await
            .map_err(GetAssertionError::ShardPeerSelection)
        } else {
            Ok(all_shard_peers)
        }
    }
}

/// Try to fetch an assertion from the local triple store, validating it before returning.
///
/// Returns `Some(assertion)` if a valid assertion was found locally, `None` if not found,
/// empty, or validation failed. Callers should fall back to network fetch on `None`.
pub(crate) async fn fetch_assertion_from_local(
    triple_store_assertions: &TripleStoreAssertions,
    assertion_validation: &AssertionValidation,
    parsed_ual: &ParsedUal,
    token_ids: &TokenIds,
    visibility: Visibility,
) -> Option<Assertion> {
    let result = triple_store_assertions
        .query_assertion(parsed_ual, token_ids, visibility, false)
        .await
        .ok()??;

    if !result.assertion.has_data() {
        return None;
    }

    if assertion_validation
        .validate_response(&result.assertion, parsed_ual, visibility)
        .await
    {
        Some(result.assertion)
    } else {
        None
    }
}

pub(crate) async fn resolve_token_ids(
    blockchain_manager: &BlockchainManager,
    operation_id: Uuid,
    parsed_ual: &ParsedUal,
    policy: TokenRangeResolutionPolicy,
) -> Result<TokenIds, GetAssertionError> {
    if let Some(token_id) = parsed_ual.knowledge_asset_id {
        return Ok(TokenIds::single(token_id as u64));
    }

    let chain_result = blockchain_manager
        .get_knowledge_assets_range(
            &parsed_ual.blockchain,
            parsed_ual.contract,
            parsed_ual.knowledge_collection_id,
        )
        .await;

    resolve_token_ids_from_chain_result(operation_id, parsed_ual, policy, chain_result)
}

fn resolve_token_ids_from_chain_result<E: std::fmt::Display>(
    operation_id: Uuid,
    parsed_ual: &ParsedUal,
    policy: TokenRangeResolutionPolicy,
    chain_result: Result<Option<(u64, u64, Vec<u64>)>, E>,
) -> Result<TokenIds, GetAssertionError> {
    match chain_result {
        Ok(Some((start, end, burned))) => {
            tracing::debug!(
                operation_id = %operation_id,
                start,
                end,
                burned = burned.len(),
                "Resolved KC token range from chain"
            );
            Ok(TokenIds::from_global_range(
                parsed_ual.knowledge_collection_id,
                start,
                end,
                burned,
            ))
        }
        Ok(None) => Ok(TokenIds::single(1)),
        Err(e) => match policy {
            TokenRangeResolutionPolicy::Strict => Err(GetAssertionError::TokenRangeResolution {
                operation_id,
                reason: e.to_string(),
            }),
            TokenRangeResolutionPolicy::CompatibleSingleTokenFallback => {
                tracing::warn!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to resolve token range, using fallback"
                );
                Ok(TokenIds::single(1))
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use dkg_domain::parse_ual;

    use super::*;

    fn test_kc_ual() -> ParsedUal {
        parse_ual("did:dkg:hardhat1:31337/0x6C1AeF3601cd0e04cD5e8E70e7ea2c11D2eF60f4/2")
            .expect("valid test UAL")
    }

    #[test]
    fn token_policy_strict_returns_error() {
        let parsed = test_kc_ual();
        let result = resolve_token_ids_from_chain_result(
            Uuid::nil(),
            &parsed,
            TokenRangeResolutionPolicy::Strict,
            Err("rpc failure"),
        );

        assert!(result.is_err());
    }

    #[test]
    fn token_policy_compatible_falls_back_to_single_token() {
        let parsed = test_kc_ual();
        let result = resolve_token_ids_from_chain_result(
            Uuid::nil(),
            &parsed,
            TokenRangeResolutionPolicy::CompatibleSingleTokenFallback,
            Err("rpc failure"),
        )
        .expect("compatible policy should fall back");

        assert_eq!(result.start_token_id(), 1);
        assert_eq!(result.end_token_id(), 1);
        assert!(result.burned().is_empty());
    }

    #[test]
    fn token_ids_convert_from_global_range() {
        let parsed = test_kc_ual();
        let result = resolve_token_ids_from_chain_result(
            Uuid::nil(),
            &parsed,
            TokenRangeResolutionPolicy::Strict,
            Ok::<Option<(u64, u64, Vec<u64>)>, &str>(Some((1_000_001, 1_000_004, vec![1_000_002]))),
        )
        .expect("on-chain range should resolve");

        assert_eq!(result.start_token_id(), 1);
        assert_eq!(result.end_token_id(), 4);
        assert_eq!(result.burned(), &[2]);
    }

    #[test]
    fn token_ids_none_from_chain_defaults_to_single_token() {
        let parsed = test_kc_ual();
        let result = resolve_token_ids_from_chain_result(
            Uuid::nil(),
            &parsed,
            TokenRangeResolutionPolicy::Strict,
            Ok::<Option<(u64, u64, Vec<u64>)>, &str>(None),
        )
        .expect("none from chain should default");

        assert_eq!(result.start_token_id(), 1);
        assert_eq!(result.end_token_id(), 1);
        assert!(result.burned().is_empty());
    }
}
