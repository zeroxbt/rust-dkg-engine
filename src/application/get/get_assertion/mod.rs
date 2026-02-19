use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_domain::{ParsedUal, Visibility, parse_ual};
use dkg_network::{NetworkManager, PeerId, STREAM_PROTOCOL_GET};
use uuid::Uuid;

use super::paranet_policy::filter_shard_peers_for_paranet;
use crate::application::{
    AssertionRetrieval, AssertionSource, FetchRequest, TokenRangeResolutionPolicy,
};
use crate::node_state::PeerRegistry;

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
    pub assertion: dkg_domain::Assertion,
    pub metadata: Option<Vec<String>>,
    pub source: AssertionSource,
}

pub(crate) struct GetAssertionUseCase {
    assertion_retrieval: Arc<AssertionRetrieval>,
    blockchain_manager: Arc<BlockchainManager>,
    network_manager: Arc<NetworkManager>,
    peer_registry: Arc<PeerRegistry>,
}

impl GetAssertionUseCase {
    pub(crate) fn new(
        assertion_retrieval: Arc<AssertionRetrieval>,
        blockchain_manager: Arc<BlockchainManager>,
        network_manager: Arc<NetworkManager>,
        peer_registry: Arc<PeerRegistry>,
    ) -> Self {
        Self {
            assertion_retrieval,
            blockchain_manager,
            network_manager,
            peer_registry,
        }
    }

    pub(crate) async fn fetch(
        &self,
        request: &GetAssertionInput,
    ) -> Result<GetAssertionOutput, String> {
        let parsed_ual = parse_ual(&request.ual).map_err(|e| format!("Invalid UAL: {}", e))?;

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
                return Err(format!(
                    "Knowledge collection {} does not exist on blockchain {}",
                    parsed_ual.knowledge_collection_id, parsed_ual.blockchain
                ));
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
            .assertion_retrieval
            .resolve_token_ids(
                request.operation_id,
                &parsed_ual,
                TokenRangeResolutionPolicy::CompatibleSingleTokenFallback,
            )
            .await
            .map_err(|e| e.to_string())?;

        let peers = self
            .load_peers_for_get(&parsed_ual, request.paranet_ual.as_deref())
            .await?;

        let fetch_request = FetchRequest {
            operation_id: request.operation_id,
            parsed_ual,
            token_ids,
            peers,
            visibility: request.visibility,
            include_metadata: request.include_metadata,
            paranet_ual: request.paranet_ual.clone(),
        };

        let fetched = self
            .assertion_retrieval
            .fetch(&fetch_request)
            .await
            .map_err(|e| e.to_string())?;

        Ok(GetAssertionOutput {
            assertion: fetched.assertion,
            metadata: fetched.metadata,
            source: fetched.source,
        })
    }

    async fn load_peers_for_get(
        &self,
        parsed_ual: &ParsedUal,
        paranet_ual: Option<&str>,
    ) -> Result<Vec<PeerId>, String> {
        let local_peer_id = self.network_manager.peer_id();
        let peers = self.peer_registry.select_shard_peers(
            &parsed_ual.blockchain,
            STREAM_PROTOCOL_GET,
            Some(local_peer_id),
        );

        if let Some(paranet_ual) = paranet_ual {
            filter_shard_peers_for_paranet(&self.blockchain_manager, parsed_ual, paranet_ual, peers)
                .await
                .map_err(|e| e.to_string())
        } else {
            Ok(peers)
        }
    }
}
