use std::sync::Arc;

use futures::{StreamExt, stream::FuturesUnordered};
use libp2p::PeerId;
use uuid::Uuid;

use crate::{
    managers::{
        blockchain::BlockchainManager,
        network::{
            NetworkError, NetworkManager,
            message::ResponseBody,
            messages::{GetRequestData, GetResponseData},
            protocols::{GetProtocol, ProtocolSpec},
        },
    },
    services::{AssertionValidationService, PeerService, TripleStoreService},
    types::{Assertion, ParsedUal, TokenIds, Visibility, parse_ual},
    utils::paranet::{construct_knowledge_collection_onchain_id, construct_paranet_id},
};

const CONCURRENT_PEERS: usize = 3;

#[derive(Debug, Clone, Copy)]
pub(crate) enum GetFetchSource {
    Local,
    Network,
}

#[derive(Debug, Clone)]
pub(crate) struct GetFetchRequest {
    pub operation_id: Uuid,
    pub ual: String,
    pub include_metadata: bool,
    pub paranet_ual: Option<String>,
    pub visibility: Visibility,
}

#[derive(Debug, Clone)]
pub(crate) struct GetFetchResult {
    pub assertion: Assertion,
    pub metadata: Option<Vec<String>>,
    pub source: GetFetchSource,
}

pub(crate) struct GetFetchService {
    blockchain_manager: Arc<BlockchainManager>,
    triple_store_service: Arc<TripleStoreService>,
    network_manager: Arc<NetworkManager>,
    assertion_validation_service: Arc<AssertionValidationService>,
    peer_service: Arc<PeerService>,
}

impl GetFetchService {
    pub(crate) fn new(
        blockchain_manager: Arc<BlockchainManager>,
        triple_store_service: Arc<TripleStoreService>,
        network_manager: Arc<NetworkManager>,
        assertion_validation_service: Arc<AssertionValidationService>,
        peer_service: Arc<PeerService>,
    ) -> Self {
        Self {
            blockchain_manager,
            triple_store_service,
            network_manager,
            assertion_validation_service,
            peer_service,
        }
    }

    pub(crate) async fn fetch(&self, request: &GetFetchRequest) -> Result<GetFetchResult, String> {
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
            .resolve_token_ids(request.operation_id, &parsed_ual)
            .await;

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

    async fn resolve_token_ids(&self, operation_id: Uuid, parsed_ual: &ParsedUal) -> TokenIds {
        if let Some(token_id) = parsed_ual.knowledge_asset_id {
            return TokenIds::single(token_id as u64);
        }

        match self
            .blockchain_manager
            .get_knowledge_assets_range(
                &parsed_ual.blockchain,
                parsed_ual.contract,
                parsed_ual.knowledge_collection_id,
            )
            .await
        {
            Ok(Some((start, end, burned))) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    start,
                    end,
                    burned = burned.len(),
                    "Resolved KC token range from chain"
                );
                TokenIds::from_global_range(parsed_ual.knowledge_collection_id, start, end, burned)
            }
            Ok(None) => TokenIds::single(1),
            Err(e) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to resolve token range, using fallback"
                );
                TokenIds::single(1)
            }
        }
    }

    async fn try_local(
        &self,
        operation_id: Uuid,
        parsed_ual: &ParsedUal,
        token_ids: &TokenIds,
        visibility: Visibility,
        include_metadata: bool,
    ) -> Option<GetFetchResult> {
        let local_result = self
            .triple_store_service
            .query_assertion(parsed_ual, token_ids, visibility, include_metadata)
            .await
            .ok()?;

        let result = local_result?;
        if !result.assertion.has_data() {
            return None;
        }

        let is_valid = self
            .assertion_validation_service
            .validate_response(&result.assertion, parsed_ual, visibility)
            .await;

        if !is_valid {
            tracing::debug!(
                operation_id = %operation_id,
                "Local assertion validation failed"
            );
            return None;
        }

        Some(GetFetchResult {
            assertion: Assertion::new(
                result.assertion.public.clone(),
                result.assertion.private.clone(),
            ),
            metadata: result.metadata.clone(),
            source: GetFetchSource::Local,
        })
    }

    async fn try_network(
        &self,
        operation_id: Uuid,
        parsed_ual: &ParsedUal,
        token_ids: TokenIds,
        request: &GetFetchRequest,
    ) -> Result<GetFetchResult, String> {
        let mut peers = self
            .load_shard_peers(parsed_ual, request.paranet_ual.as_deref())
            .await?;

        if peers.is_empty() {
            return Err(format!(
                "Unable to find enough nodes for operation: {}. Found 0 nodes, need at least 1",
                operation_id
            ));
        }

        self.peer_service.sort_by_latency(&mut peers);

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

        let mut futures = FuturesUnordered::new();
        let mut peers_iter = peers.iter().cloned();
        let limit = CONCURRENT_PEERS.max(1).min(peers.len());
        for _ in 0..limit {
            if let Some(peer) = peers_iter.next() {
                futures.push(send_get_request_to_peer(
                    Arc::clone(&self.network_manager),
                    peer,
                    operation_id,
                    get_request_data.clone(),
                ));
            }
        }

        while let Some((peer, response)) = futures.next().await {
            if let Some(result) = self
                .validate_response(&peer, response, parsed_ual, request.visibility)
                .await
            {
                return Ok(result);
            }

            if let Some(peer) = peers_iter.next() {
                futures.push(send_get_request_to_peer(
                    Arc::clone(&self.network_manager),
                    peer,
                    operation_id,
                    get_request_data.clone(),
                ));
            }
        }

        Err(format!(
            "Failed to get data from network for operation: {}",
            operation_id
        ))
    }

    async fn validate_response(
        &self,
        peer: &PeerId,
        response: Result<GetResponseData, NetworkError>,
        parsed_ual: &ParsedUal,
        visibility: Visibility,
    ) -> Option<GetFetchResult> {
        let response = match response {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(peer = %peer, error = %e, "Get request failed");
                return None;
            }
        };

        match response {
            ResponseBody::Ack(ack) => {
                let is_valid = self
                    .assertion_validation_service
                    .validate_response(&ack.assertion, parsed_ual, visibility)
                    .await;
                if !is_valid {
                    tracing::debug!(peer = %peer, "Ack failed assertion validation");
                    return None;
                }

                Some(GetFetchResult {
                    assertion: Assertion::new(
                        ack.assertion.public.clone(),
                        ack.assertion.private.clone(),
                    ),
                    metadata: ack.metadata.clone(),
                    source: GetFetchSource::Network,
                })
            }
            ResponseBody::Error(err) => {
                tracing::debug!(peer = %peer, error = %err.error_message, "Peer returned NACK");
                None
            }
        }
    }

    async fn load_shard_peers(
        &self,
        parsed_ual: &ParsedUal,
        paranet_ual: Option<&str>,
    ) -> Result<Vec<PeerId>, String> {
        let my_peer_id = self.network_manager.peer_id();
        let all_shard_peers = self.peer_service.select_shard_peers(
            &parsed_ual.blockchain,
            GetProtocol::STREAM_PROTOCOL,
            Some(my_peer_id),
        );

        if let Some(paranet_ual) = paranet_ual {
            self.handle_paranet_node_selection(paranet_ual, parsed_ual, all_shard_peers)
                .await
        } else {
            Ok(all_shard_peers)
        }
    }

    async fn handle_paranet_node_selection(
        &self,
        paranet_ual: &str,
        target_ual: &ParsedUal,
        all_shard_peers: Vec<PeerId>,
    ) -> Result<Vec<PeerId>, String> {
        let paranet_parsed =
            parse_ual(paranet_ual).map_err(|e| format!("Invalid paranet UAL: {}", e))?;
        let ka_id = paranet_parsed
            .knowledge_asset_id
            .ok_or_else(|| "Paranet UAL must include knowledge asset ID".to_string())?;

        let paranet_id = construct_paranet_id(
            paranet_parsed.contract,
            paranet_parsed.knowledge_collection_id,
            ka_id,
        );

        let exists = self
            .blockchain_manager
            .paranet_exists(&target_ual.blockchain, paranet_id)
            .await
            .map_err(|e| format!("Failed to check paranet existence: {}", e))?;
        if !exists {
            return Err(format!("Paranet does not exist: {}", paranet_ual));
        }

        let policy = self
            .blockchain_manager
            .get_nodes_access_policy(&target_ual.blockchain, paranet_id)
            .await
            .map_err(|e| format!("Failed to get access policy: {}", e))?;

        let kc_onchain_id = construct_knowledge_collection_onchain_id(
            target_ual.contract,
            target_ual.knowledge_collection_id,
        );
        let kc_registered = self
            .blockchain_manager
            .is_knowledge_collection_registered(&target_ual.blockchain, paranet_id, kc_onchain_id)
            .await
            .map_err(|e| format!("Failed to check KC registration in paranet: {}", e))?;
        if !kc_registered {
            return Err("Knowledge collection not registered in paranet".to_string());
        }

        match policy {
            crate::types::AccessPolicy::Permissioned => {
                let permissioned_nodes = self
                    .blockchain_manager
                    .get_permissioned_nodes(&target_ual.blockchain, paranet_id)
                    .await
                    .map_err(|e| format!("Failed to get permissioned nodes: {}", e))?;

                let permissioned_peer_ids: std::collections::HashSet<PeerId> = permissioned_nodes
                    .iter()
                    .filter_map(|node| {
                        String::from_utf8(node.nodeId.to_vec())
                            .ok()
                            .and_then(|s| s.parse::<PeerId>().ok())
                    })
                    .collect();

                Ok(all_shard_peers
                    .into_iter()
                    .filter(|peer_id| permissioned_peer_ids.contains(peer_id))
                    .collect())
            }
            crate::types::AccessPolicy::Open => Ok(all_shard_peers),
        }
    }
}

async fn send_get_request_to_peer(
    network_manager: Arc<NetworkManager>,
    peer: PeerId,
    operation_id: Uuid,
    request_data: GetRequestData,
) -> (PeerId, Result<GetResponseData, NetworkError>) {
    let result = network_manager
        .send_get_request(peer, operation_id, request_data)
        .await;
    (peer, result)
}
