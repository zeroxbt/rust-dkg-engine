mod config;
mod network_fetch;

use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_domain::{Assertion, ParsedUal, TokenIds, Visibility};
use dkg_network::{GetRequestData, NetworkManager, PeerId};
use uuid::Uuid;

use crate::application::{AssertionValidation, TripleStoreAssertions};

pub(crate) use config::NETWORK_CONCURRENT_PEERS;

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

#[derive(Debug, Clone)]
pub(crate) struct FetchRequest {
    pub operation_id: Uuid,
    pub parsed_ual: ParsedUal,
    pub token_ids: TokenIds,
    pub peers: Vec<PeerId>,
    pub visibility: Visibility,
    pub include_metadata: bool,
    pub paranet_ual: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct FetchedAssertion {
    pub assertion: Assertion,
    pub metadata: Option<Vec<String>>,
    pub source: AssertionSource,
}

#[derive(Debug)]
pub(crate) enum RetrievalError {
    TokenResolution(String),
    NoPeers { operation_id: Uuid },
    NotFound { operation_id: Uuid },
}

impl std::fmt::Display for RetrievalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TokenResolution(msg) => write!(f, "{}", msg),
            Self::NoPeers { operation_id } => write!(
                f,
                "Unable to find enough nodes for operation: {}. Found 0 nodes, need at least 1",
                operation_id
            ),
            Self::NotFound { operation_id } => {
                write!(
                    f,
                    "Failed to get data from network for operation: {}",
                    operation_id
                )
            }
        }
    }
}

pub(crate) struct AssertionRetrieval {
    blockchain_manager: Arc<BlockchainManager>,
    triple_store_assertions: Arc<TripleStoreAssertions>,
    network_manager: Arc<NetworkManager>,
    assertion_validation: Arc<AssertionValidation>,
}

impl AssertionRetrieval {
    pub(crate) fn new(
        blockchain_manager: Arc<BlockchainManager>,
        triple_store_assertions: Arc<TripleStoreAssertions>,
        network_manager: Arc<NetworkManager>,
        assertion_validation: Arc<AssertionValidation>,
    ) -> Self {
        Self {
            blockchain_manager,
            triple_store_assertions,
            network_manager,
            assertion_validation,
        }
    }

    pub(crate) async fn resolve_token_ids(
        &self,
        operation_id: Uuid,
        parsed_ual: &ParsedUal,
        policy: TokenRangeResolutionPolicy,
    ) -> Result<TokenIds, RetrievalError> {
        if let Some(token_id) = parsed_ual.knowledge_asset_id {
            return Ok(TokenIds::single(token_id as u64));
        }

        let chain_result = self
            .blockchain_manager
            .get_knowledge_assets_range(
                &parsed_ual.blockchain,
                parsed_ual.contract,
                parsed_ual.knowledge_collection_id,
            )
            .await;

        resolve_token_ids_from_chain_result(operation_id, parsed_ual, policy, chain_result)
    }

    pub(crate) async fn fetch(
        &self,
        request: &FetchRequest,
    ) -> Result<FetchedAssertion, RetrievalError> {
        if let Some(local) = self.try_local(request).await {
            return Ok(local);
        }

        self.try_network(request).await
    }

    async fn try_local(&self, request: &FetchRequest) -> Option<FetchedAssertion> {
        let local_result = match self
            .triple_store_assertions
            .query_assertion(
                &request.parsed_ual,
                &request.token_ids,
                request.visibility,
                request.include_metadata,
            )
            .await
        {
            Ok(result) => result,
            Err(e) => {
                tracing::debug!(
                    operation_id = %request.operation_id,
                    error = %e,
                    "Local triple-store query failed, falling back to network"
                );
                return None;
            }
        };

        let result = local_result?;
        if !result.assertion.has_data() {
            return None;
        }

        let is_valid = self
            .assertion_validation
            .validate_response(&result.assertion, &request.parsed_ual, request.visibility)
            .await;

        if !is_valid {
            tracing::debug!(
                operation_id = %request.operation_id,
                "Local assertion validation failed"
            );
            return None;
        }

        Some(FetchedAssertion {
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
        request: &FetchRequest,
    ) -> Result<FetchedAssertion, RetrievalError> {
        if request.peers.is_empty() {
            return Err(RetrievalError::NoPeers {
                operation_id: request.operation_id,
            });
        }

        let get_request_data = GetRequestData::new(
            request.parsed_ual.blockchain.clone(),
            format!("{:?}", request.parsed_ual.contract),
            request.parsed_ual.knowledge_collection_id,
            request.parsed_ual.knowledge_asset_id,
            request.parsed_ual.to_ual_string(),
            request.token_ids.clone(),
            request.include_metadata,
            request.paranet_ual.clone(),
        );

        if let Some(ack) = network_fetch::fetch_first_valid_ack_from_peers(
            Arc::clone(&self.network_manager),
            Arc::clone(&self.assertion_validation),
            request.peers.clone(),
            request.operation_id,
            get_request_data,
            request.parsed_ual.clone(),
            request.visibility,
        )
        .await
        {
            return Ok(FetchedAssertion {
                assertion: Assertion::new(
                    ack.assertion.public.clone(),
                    ack.assertion.private.clone(),
                ),
                metadata: ack.metadata.clone(),
                source: AssertionSource::Network,
            });
        }

        Err(RetrievalError::NotFound {
            operation_id: request.operation_id,
        })
    }
}

fn resolve_token_ids_from_chain_result<E: std::fmt::Display>(
    operation_id: Uuid,
    parsed_ual: &ParsedUal,
    policy: TokenRangeResolutionPolicy,
    chain_result: Result<Option<(u64, u64, Vec<u64>)>, E>,
) -> Result<TokenIds, RetrievalError> {
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
            TokenRangeResolutionPolicy::Strict => Err(RetrievalError::TokenResolution(format!(
                "Failed to resolve token range for operation {}: {}",
                operation_id, e
            ))),
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
