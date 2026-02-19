use dkg_blockchain::{B256, BlockchainId, BlockchainManager, keccak256_encode_packed};
use dkg_domain::SignatureComponents;
use uuid::Uuid;

use crate::{
    application::OperationTracking,
    error::NodeError,
    operations::{PublishStoreOperation, PublishStoreOperationResult, PublishStoreSignatureData},
};

pub(crate) fn to_publish_store_signature_data(
    identity_id: u128,
    signature: &SignatureComponents,
) -> PublishStoreSignatureData {
    PublishStoreSignatureData::new(
        identity_id.to_string(),
        signature.v,
        signature.r.clone(),
        signature.s.clone(),
        signature.vs.clone(),
    )
}

pub(crate) async fn append_network_signature(
    publish_store_operation_tracking: &OperationTracking<PublishStoreOperation>,
    operation_id: Uuid,
    signature_data: PublishStoreSignatureData,
) -> Result<(), NodeError> {
    publish_store_operation_tracking
        .update_result(
            operation_id,
            PublishStoreOperationResult::new(None, Vec::new()),
            move |result| {
                result.network_signatures.push(signature_data);
            },
        )
        .await?;

    Ok(())
}

pub(crate) async fn store_publisher_signature(
    publish_store_operation_tracking: &OperationTracking<PublishStoreOperation>,
    operation_id: Uuid,
    signature_data: PublishStoreSignatureData,
) -> Result<(), NodeError> {
    publish_store_operation_tracking
        .update_result(
            operation_id,
            PublishStoreOperationResult::new(None, Vec::new()),
            move |result| {
                result.publisher_signature = Some(signature_data);
            },
        )
        .await?;

    Ok(())
}

pub(crate) async fn sign_dataset_root_hex(
    blockchain_manager: &BlockchainManager,
    blockchain: &BlockchainId,
    dataset_root_hex: &str,
) -> Result<SignatureComponents, NodeError> {
    Ok(blockchain_manager
        .sign_message(blockchain, dataset_root_hex)
        .await?)
}

pub(crate) async fn create_publisher_signature(
    blockchain_manager: &BlockchainManager,
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
    let message_hash = keccak256_encode_packed(&[&identity_bytes, dataset_root_b256.as_slice()]);
    let signature = blockchain_manager
        .sign_message(
            blockchain,
            &format!("0x{}", dkg_blockchain::to_hex_string(message_hash)),
        )
        .await?;

    Ok(to_publish_store_signature_data(identity_id, &signature))
}
