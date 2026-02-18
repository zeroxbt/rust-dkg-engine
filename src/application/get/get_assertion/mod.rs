use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_domain::{ParsedUal, Visibility, parse_ual};
use dkg_network::{PeerId, STREAM_PROTOCOL_GET};
use uuid::Uuid;

use crate::application::{
    AssertionRetrieval, AssertionSource, FetchRequest, ParanetAccessResolution, ShardPeerSelection,
    TokenRangeResolutionPolicy, resolve_paranet_access,
};

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
    shard_peer_selection: Arc<ShardPeerSelection>,
}

impl GetAssertionUseCase {
    pub(crate) fn new(
        assertion_retrieval: Arc<AssertionRetrieval>,
        blockchain_manager: Arc<BlockchainManager>,
        shard_peer_selection: Arc<ShardPeerSelection>,
    ) -> Self {
        Self {
            assertion_retrieval,
            blockchain_manager,
            shard_peer_selection,
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
        let peers = self
            .shard_peer_selection
            .load_shard_peers(&parsed_ual.blockchain, STREAM_PROTOCOL_GET);

        if let Some(paranet_ual) = paranet_ual {
            self.filter_peers_by_paranet(paranet_ual, parsed_ual, peers)
                .await
        } else {
            Ok(peers)
        }
    }

    async fn filter_peers_by_paranet(
        &self,
        paranet_ual: &str,
        target_ual: &ParsedUal,
        all_shard_peers: Vec<PeerId>,
    ) -> Result<Vec<PeerId>, String> {
        match resolve_paranet_access(&self.blockchain_manager, target_ual, paranet_ual, true)
            .await
            .map_err(|e| e.to_string())?
        {
            ParanetAccessResolution::Permissioned {
                permissioned_peer_ids,
                ..
            } => Ok(all_shard_peers
                .into_iter()
                .filter(|peer_id| permissioned_peer_ids.contains(peer_id))
                .collect()),
            ParanetAccessResolution::Open { .. } => Ok(all_shard_peers),
        }
    }
}
