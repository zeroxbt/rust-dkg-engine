use std::sync::Arc;

use dkg_blockchain::{BlockchainId, BlockchainManager};
use dkg_domain::{Assertion, SignatureComponents};
use dkg_key_value_store::{PublishTmpDataset, PublishTmpDatasetStore};
use dkg_network::PeerId;
use uuid::Uuid;

use crate::node_state::PeerRegistry;

#[derive(Debug, Clone)]
pub(crate) struct ServePublishStoreInput {
    pub blockchain: BlockchainId,
    pub operation_id: Uuid,
    pub dataset_root: String,
    pub remote_peer_id: PeerId,
    pub local_peer_id: PeerId,
    pub dataset: Assertion,
}

#[derive(Debug, Clone)]
pub(crate) enum ServePublishStoreOutcome {
    Ack {
        identity_id: u128,
        signature: SignatureComponents,
    },
    Nack {
        error_message: String,
    },
}

pub(crate) struct ServePublishStoreWorkflow {
    blockchain_manager: Arc<BlockchainManager>,
    peer_registry: Arc<PeerRegistry>,
    publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
}

impl ServePublishStoreWorkflow {
    pub(crate) fn new(
        blockchain_manager: Arc<BlockchainManager>,
        peer_registry: Arc<PeerRegistry>,
        publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
    ) -> Self {
        Self {
            blockchain_manager,
            peer_registry,
            publish_tmp_dataset_store,
        }
    }

    pub(crate) async fn execute(&self, input: &ServePublishStoreInput) -> ServePublishStoreOutcome {
        if let Some(missing_blockchain) = self
            .peer_registry
            .first_missing_shard_membership(&input.local_peer_id, [&input.blockchain])
        {
            tracing::warn!(
                operation_id = %input.operation_id,
                local_peer_id = %input.local_peer_id,
                blockchain = %missing_blockchain,
                "Local node not found in shard"
            );
            return ServePublishStoreOutcome::Nack {
                error_message: "Local node not in shard".to_string(),
            };
        }

        let computed_dataset_root = dkg_domain::calculate_merkle_root(&input.dataset.public);
        if input.dataset_root != computed_dataset_root {
            tracing::warn!(
                operation_id = %input.operation_id,
                received = %input.dataset_root,
                computed = %computed_dataset_root,
                "Dataset root mismatch"
            );
            return ServePublishStoreOutcome::Nack {
                error_message: format!(
                    "Dataset root validation failed. Received dataset root: {}; Calculated dataset root: {}",
                    input.dataset_root, computed_dataset_root
                ),
            };
        }

        let identity_id = self.blockchain_manager.identity_id(&input.blockchain);

        let Some(dataset_root_hex) = input.dataset_root.strip_prefix("0x") else {
            return ServePublishStoreOutcome::Nack {
                error_message: "Dataset root missing '0x' prefix".to_string(),
            };
        };

        let signature = match self
            .blockchain_manager
            .sign_message(&input.blockchain, dataset_root_hex)
            .await
        {
            Ok(sig) => sig,
            Err(e) => {
                tracing::error!(
                    operation_id = %input.operation_id,
                    error = %e,
                    "Failed to sign message"
                );
                return ServePublishStoreOutcome::Nack {
                    error_message: format!("Failed to sign message: {}", e),
                };
            }
        };

        let pending = PublishTmpDataset::new(
            input.dataset_root.clone(),
            input.dataset.clone(),
            input.remote_peer_id.to_base58(),
        );
        if let Err(e) = self
            .publish_tmp_dataset_store
            .store(input.operation_id, pending)
            .await
        {
            tracing::error!(
                operation_id = %input.operation_id,
                error = %e,
                "Failed to store dataset in publish tmp dataset store"
            );
            return ServePublishStoreOutcome::Nack {
                error_message: format!("Failed to store dataset: {}", e),
            };
        }

        ServePublishStoreOutcome::Ack {
            identity_id,
            signature,
        }
    }
}
