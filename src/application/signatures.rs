use dkg_blockchain::{
    B256, BlockchainId, BlockchainManager, keccak256_encode_packed, to_hex_string,
};
use dkg_domain::SignatureComponents;
use dkg_key_value_store::PublishStoreSignatureData;

#[derive(Debug, Clone)]
pub(crate) enum SignatureError {
    DatasetRootMissingHexPrefix,
    InvalidDatasetRootHex(String),
    FailedToSignMessage(String),
}

impl std::fmt::Display for SignatureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DatasetRootMissingHexPrefix => write!(f, "Dataset root missing '0x' prefix"),
            Self::InvalidDatasetRootHex(error) => {
                write!(f, "Invalid dataset root hex: {}", error)
            }
            Self::FailedToSignMessage(error) => write!(f, "Failed to sign message: {}", error),
        }
    }
}

pub(crate) fn dataset_root_payload(dataset_root: &str) -> Result<&str, SignatureError> {
    dataset_root
        .strip_prefix("0x")
        .ok_or(SignatureError::DatasetRootMissingHexPrefix)
}

pub(crate) async fn sign_dataset_root(
    blockchain_manager: &BlockchainManager,
    blockchain: &BlockchainId,
    dataset_root: &str,
) -> Result<SignatureComponents, SignatureError> {
    let payload = dataset_root_payload(dataset_root)?;
    blockchain_manager
        .sign_message(blockchain, payload)
        .await
        .map_err(|e| SignatureError::FailedToSignMessage(e.to_string()))
}

pub(crate) async fn create_publisher_signature(
    blockchain_manager: &BlockchainManager,
    blockchain: &BlockchainId,
    dataset_root: &str,
    identity_id: u128,
) -> Result<PublishStoreSignatureData, SignatureError> {
    let dataset_root_b256: B256 = dataset_root
        .parse::<B256>()
        .map_err(|e| SignatureError::InvalidDatasetRootHex(e.to_string()))?;

    let message_hash = publisher_message_hash(identity_id, dataset_root_b256);
    let signature = blockchain_manager
        .sign_message(blockchain, &format!("0x{}", to_hex_string(message_hash)))
        .await
        .map_err(|e| SignatureError::FailedToSignMessage(e.to_string()))?;

    Ok(signature_data_from_components(identity_id, &signature))
}

pub(crate) fn signature_data_from_components(
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

fn publisher_message_hash(identity_id: u128, dataset_root: B256) -> [u8; 32] {
    let identity_bytes = identity_id_bytes(identity_id);
    keccak256_encode_packed(&[&identity_bytes, dataset_root.as_slice()])
}

fn identity_id_bytes(identity_id: u128) -> [u8; 9] {
    let bytes = identity_id.to_be_bytes();
    let mut out = [0u8; 9];
    out.copy_from_slice(&bytes[16 - 9..]);
    out
}

#[cfg(test)]
mod tests {
    use super::{dataset_root_payload, identity_id_bytes};

    #[test]
    fn payload_requires_0x_prefix() {
        assert_eq!(dataset_root_payload("0xabc").ok(), Some("abc"));
        assert!(dataset_root_payload("abc").is_err());
    }

    #[test]
    fn identity_id_is_encoded_to_last_nine_bytes() {
        let bytes = identity_id_bytes(0x112233445566778899u128);
        assert_eq!(
            bytes,
            [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99]
        );
    }
}
