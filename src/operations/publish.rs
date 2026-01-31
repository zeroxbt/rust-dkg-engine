use serde::{Deserialize, Serialize};
/// Signature data stored after Publish store operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PublishStoreSignatureData {
    pub identity_id: String,
    pub v: u8,
    pub r: String,
    pub s: String,
    pub vs: String,
}

impl PublishStoreSignatureData {
    pub(crate) fn new(identity_id: String, v: u8, r: String, s: String, vs: String) -> Self {
        Self {
            identity_id,
            v,
            r,
            s,
            vs,
        }
    }
}

/// Result stored after successful Publish operation.
///
/// Contains all signatures collected during the publish operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PublishStoreOperationResult {
    /// The publisher's own signature over the dataset
    pub publisher_signature: Option<PublishStoreSignatureData>,
    /// Signatures from network nodes that stored the dataset
    pub network_signatures: Vec<PublishStoreSignatureData>,
}

impl PublishStoreOperationResult {
    pub(crate) fn new(
        publisher_signature: Option<PublishStoreSignatureData>,
        network_signatures: Vec<PublishStoreSignatureData>,
    ) -> Self {
        Self {
            publisher_signature,
            network_signatures,
        }
    }
}
