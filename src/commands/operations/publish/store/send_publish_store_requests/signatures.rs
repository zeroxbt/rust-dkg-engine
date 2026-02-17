use dkg_blockchain::{B256, BlockchainId, keccak256_encode_packed};
use uuid::Uuid;

use super::handler::SendPublishStoreRequestsCommandHandler;
use crate::{
    error::NodeError,
    operations::{PublishStoreOperationResult, PublishStoreSignatureData},
};

impl SendPublishStoreRequestsCommandHandler {
    /// Handle self-node signature (when publisher is in the shard).
    /// Stores the signature directly to redb and records a successful response.
    pub(crate) async fn handle_self_node_signature(
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

        // Add self-node signature directly to redb as a network signature
        let sig_data = PublishStoreSignatureData::new(
            identity_id.to_string(),
            signature.v,
            signature.r.clone(),
            signature.s.clone(),
            signature.vs.clone(),
        );

        self.publish_store_operation_status_service
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

    /// Create publisher signature and store it in context.
    /// Returns the PublishStoreSignatureData for storage in context.
    pub(crate) async fn create_publisher_signature(
        &self,
        blockchain: &BlockchainId,
        dataset_root: &str,
        identity_id: u128,
    ) -> Result<PublishStoreSignatureData, NodeError> {
        let dataset_root_b256: B256 = dataset_root
            .parse()
            .map_err(|e| NodeError::Other(format!("Invalid dataset root hex: {e}")))?;
        // JS uses: keccak256EncodePacked(['uint72', 'bytes32'], [identityId, datasetRoot])
        // uint72 = 9 bytes, so we encode identity_id as FixedBytes(9) to match Solidity's packed
        // encoding
        let identity_bytes = {
            let bytes = identity_id.to_be_bytes(); // u128 = 16 bytes
            let mut out = [0u8; 9];
            out.copy_from_slice(&bytes[16 - 9..]); // Take last 9 bytes for uint72
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
