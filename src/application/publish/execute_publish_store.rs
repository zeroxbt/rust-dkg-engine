use std::sync::Arc;

use dkg_blockchain::{B256, BlockchainId, BlockchainManager, keccak256_encode_packed};
use dkg_domain::Assertion;
use dkg_key_value_store::{PublishTmpDataset, PublishTmpDatasetStore};
use dkg_network::{
    NetworkError, NetworkManager, PeerId, STREAM_PROTOCOL_STORE, StoreRequestData,
    StoreResponseData,
};
use futures::{StreamExt, stream::FuturesUnordered};
use uuid::Uuid;

use crate::{
    application::OperationTracking,
    error::NodeError,
    node_state::PeerRegistry,
    operations::{PublishStoreOperation, PublishStoreOperationResult, PublishStoreSignatureData},
};

/// Maximum number of in-flight peer requests for this operation.
const CONCURRENT_PEERS: usize = usize::MAX;

#[derive(Debug, Clone)]
pub(crate) struct ExecutePublishStoreInput {
    pub operation_id: Uuid,
    pub blockchain: BlockchainId,
    pub dataset_root: String,
    pub min_ack_responses: u8,
    pub dataset: Assertion,
}

pub(crate) struct ExecutePublishStoreWorkflow {
    network_manager: Arc<NetworkManager>,
    peer_registry: Arc<PeerRegistry>,
    blockchain_manager: Arc<BlockchainManager>,
    publish_store_operation_tracking: Arc<OperationTracking<PublishStoreOperation>>,
    publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
}

impl ExecutePublishStoreWorkflow {
    pub(crate) fn new(
        network_manager: Arc<NetworkManager>,
        peer_registry: Arc<PeerRegistry>,
        blockchain_manager: Arc<BlockchainManager>,
        publish_store_operation_tracking: Arc<OperationTracking<PublishStoreOperation>>,
        publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
    ) -> Self {
        Self {
            network_manager,
            peer_registry,
            blockchain_manager,
            publish_store_operation_tracking,
            publish_tmp_dataset_store,
        }
    }

    pub(crate) async fn execute(&self, input: &ExecutePublishStoreInput) {
        let operation_id = input.operation_id;
        let blockchain = &input.blockchain;
        let dataset_root = &input.dataset_root;
        let dataset = &input.dataset;

        // Determine effective min_ack_responses using max(chain_min, user_provided)
        let user_min = input.min_ack_responses;
        let chain_min = match self
            .blockchain_manager
            .get_minimum_required_signatures(blockchain)
            .await
        {
            Ok(min) => min as u8,
            Err(e) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    blockchain = %blockchain,
                    error = %e,
                    "Failed to fetch on-chain minimumRequiredSignatures, using default/user value"
                );
                0
            }
        };

        let min_ack_responses = chain_min.max(user_min);

        let my_peer_id = *self.network_manager.peer_id();

        let self_in_shard = self.peer_registry.is_peer_in_shard(blockchain, &my_peer_id);
        let remote_peers = self.peer_registry.select_shard_peers(
            blockchain,
            STREAM_PROTOCOL_STORE,
            Some(&my_peer_id),
        );

        tracing::debug!(
            operation_id = %operation_id,
            remote_peers_count = remote_peers.len(),
            self_in_shard = self_in_shard,
            "Retrieved shard peers from peer service"
        );

        let total_peers = if self_in_shard {
            remote_peers.len() as u16 + 1
        } else {
            remote_peers.len() as u16
        };

        tracing::debug!(
            operation_id = %operation_id,
            total_peers = total_peers,
            remote_peers = remote_peers.len(),
            self_in_shard = self_in_shard,
            my_peer_id = %my_peer_id,
            "Parsed peer IDs from shard nodes"
        );

        let min_ack_required = min_ack_responses as u16;
        let min_required_peers = min_ack_required;
        if total_peers < min_required_peers {
            let error_message = format!(
                "Unable to find enough nodes for operation: {operation_id}. Minimum number of nodes required: {min_required_peers}"
            );

            self.publish_store_operation_tracking
                .mark_failed(operation_id, error_message)
                .await;

            return;
        }

        let identity_id = self.blockchain_manager.identity_id(blockchain);

        let Some(dataset_root_hex) = dataset_root.strip_prefix("0x") else {
            self.publish_store_operation_tracking
                .mark_failed(operation_id, "Dataset root missing '0x' prefix".to_string())
                .await;
            return;
        };

        match self
            .create_publisher_signature(blockchain, dataset_root, identity_id)
            .await
        {
            Ok(sig) => {
                if let Err(e) = self
                    .publish_store_operation_tracking
                    .update_result(
                        operation_id,
                        PublishStoreOperationResult::new(None, Vec::new()),
                        |result| {
                            result.publisher_signature = Some(sig);
                        },
                    )
                    .await
                {
                    tracing::warn!(
                        operation_id = %operation_id,
                        error = %e,
                        "Failed to store publisher signature to result storage"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to create publisher signature, operation may still succeed with network signatures"
                );
            }
        }

        let pending = PublishTmpDataset::new(
            dataset_root.to_owned(),
            dataset.clone(),
            my_peer_id.to_base58(),
        );
        if let Err(e) = self
            .publish_tmp_dataset_store
            .store(operation_id, pending)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to store dataset in publish tmp dataset store"
            );
            self.publish_store_operation_tracking
                .mark_failed(operation_id, format!("Failed to store dataset: {}", e))
                .await;
            return;
        }

        if self_in_shard {
            tracing::debug!(
                operation_id = %operation_id,
                peer = %my_peer_id,
                "Processing self-node (publisher), handling signature locally"
            );
            if let Err(e) = self
                .handle_self_node_signature(operation_id, blockchain, dataset_root_hex, identity_id)
                .await
            {
                tracing::warn!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to handle self-node signature, continuing with other nodes"
                );
            } else {
                tracing::debug!(
                    operation_id = %operation_id,
                    "Self-node signature handled successfully"
                );
            }
        }

        let store_request_data = StoreRequestData::new(
            dataset.public.clone(),
            dataset_root.clone(),
            blockchain.to_owned(),
        );

        let mut success_count: u16 = if self_in_shard { 1 } else { 0 };
        let mut failure_count: u16 = 0;
        let mut reached_threshold = success_count >= min_ack_required;

        if !reached_threshold && !remote_peers.is_empty() {
            let mut futures = FuturesUnordered::new();
            let mut peers_iter = remote_peers.iter().cloned();
            let limit = CONCURRENT_PEERS.max(1).min(remote_peers.len());

            for _ in 0..limit {
                if let Some(peer) = peers_iter.next() {
                    futures.push(send_store_request_to_peer(
                        Arc::clone(&self.network_manager),
                        peer,
                        operation_id,
                        store_request_data.clone(),
                    ));
                }
            }

            while let Some((peer, result)) = futures.next().await {
                match result {
                    Ok(response) => {
                        let is_valid = self
                            .process_store_response(operation_id, &peer, &response)
                            .await;

                        if !is_valid {
                            failure_count += 1;
                        } else {
                            success_count += 1;
                        }
                    }
                    Err(e) => {
                        failure_count += 1;
                        tracing::debug!(
                            operation_id = %operation_id,
                            peer = %peer,
                            error = %e,
                            "Store request failed"
                        );
                    }
                }

                if success_count >= min_ack_required {
                    reached_threshold = true;
                    break;
                }

                if let Some(peer) = peers_iter.next() {
                    futures.push(send_store_request_to_peer(
                        Arc::clone(&self.network_manager),
                        peer,
                        operation_id,
                        store_request_data.clone(),
                    ));
                }
            }
        }

        if reached_threshold {
            tracing::info!(
                operation_id = %operation_id,
                success_count = success_count,
                failure_count = failure_count,
                "Publish store completed"
            );

            if let Err(e) = self
                .publish_store_operation_tracking
                .complete(operation_id)
                .await
            {
                tracing::error!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to complete operation"
                );
                self.publish_store_operation_tracking
                    .mark_failed(operation_id, e.to_string())
                    .await;
            }

            return;
        }

        let error_message = format!(
            "Failed to get enough signatures. Success: {}, Failed: {}, Required: {}",
            success_count, failure_count, min_ack_responses
        );
        tracing::warn!(operation_id = %operation_id, %error_message, "Publish store failed");
        self.publish_store_operation_tracking
            .mark_failed(operation_id, error_message)
            .await;
    }

    async fn process_store_response(
        &self,
        operation_id: Uuid,
        peer: &PeerId,
        response: &StoreResponseData,
    ) -> bool {
        match response {
            StoreResponseData::Ack(ack) => {
                let identity_id = ack.identity_id;
                let signature = &ack.signature;
                let sig_data = PublishStoreSignatureData::new(
                    identity_id.to_string(),
                    signature.v,
                    signature.r.clone(),
                    signature.s.clone(),
                    signature.vs.clone(),
                );

                if let Err(e) = self
                    .publish_store_operation_tracking
                    .update_result(
                        operation_id,
                        PublishStoreOperationResult::new(None, Vec::new()),
                        |result| {
                            result.network_signatures.push(sig_data);
                        },
                    )
                    .await
                {
                    tracing::error!(
                        operation_id = %operation_id,
                        peer = %peer,
                        error = %e,
                        "Failed to store network signature"
                    );
                    return false;
                }

                tracing::debug!(
                    operation_id = %operation_id,
                    peer = %peer,
                    identity_id = %identity_id,
                    "Signature stored successfully"
                );
                true
            }
            StoreResponseData::Error(err) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    peer = %peer,
                    error = %err.error_message,
                    "Peer returned error response"
                );
                false
            }
        }
    }

    async fn handle_self_node_signature(
        &self,
        operation_id: Uuid,
        blockchain: &BlockchainId,
        dataset_root_hex: &str,
        identity_id: u128,
    ) -> Result<(), NodeError> {
        let signature = self
            .blockchain_manager
            .sign_message(blockchain, dataset_root_hex)
            .await?;

        let sig_data = PublishStoreSignatureData::new(
            identity_id.to_string(),
            signature.v,
            signature.r.clone(),
            signature.s.clone(),
            signature.vs.clone(),
        );

        self.publish_store_operation_tracking
            .update_result(
                operation_id,
                PublishStoreOperationResult::new(None, Vec::new()),
                |result| {
                    result.network_signatures.push(sig_data);
                },
            )
            .await?;

        Ok(())
    }

    async fn create_publisher_signature(
        &self,
        blockchain: &BlockchainId,
        dataset_root: &str,
        identity_id: u128,
    ) -> Result<PublishStoreSignatureData, NodeError> {
        let dataset_root_b256: B256 = dataset_root
            .parse()
            .map_err(|e| NodeError::Other(format!("Invalid dataset root hex: {e}")))?;
        let identity_bytes = {
            let bytes = identity_id.to_be_bytes();
            let mut out = [0u8; 9];
            out.copy_from_slice(&bytes[16 - 9..]);
            out
        };
        let message_hash =
            keccak256_encode_packed(&[&identity_bytes, dataset_root_b256.as_slice()]);
        let signature = self
            .blockchain_manager
            .sign_message(
                blockchain,
                &format!("0x{}", dkg_blockchain::to_hex_string(message_hash)),
            )
            .await?;

        Ok(PublishStoreSignatureData::new(
            identity_id.to_string(),
            signature.v,
            signature.r,
            signature.s,
            signature.vs,
        ))
    }
}

async fn send_store_request_to_peer(
    network_manager: Arc<NetworkManager>,
    peer: PeerId,
    operation_id: Uuid,
    request_data: StoreRequestData,
) -> (PeerId, Result<StoreResponseData, NetworkError>) {
    let result = network_manager
        .send_store_request(peer, operation_id, request_data)
        .await;
    (peer, result)
}
