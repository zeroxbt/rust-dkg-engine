use serde::{Deserialize, Serialize};

use crate::types::{BlockchainId, SignatureComponents};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StoreRequestData {
    dataset: Vec<String>,
    dataset_root: String,
    blockchain: BlockchainId,
}

impl StoreRequestData {
    pub(crate) fn new(
        dataset: Vec<String>,
        dataset_root: String,
        blockchain: BlockchainId,
    ) -> Self {
        Self {
            dataset,
            dataset_root,
            blockchain,
        }
    }

    pub(crate) fn dataset(&self) -> &Vec<String> {
        &self.dataset
    }

    pub(crate) fn dataset_root(&self) -> &str {
        &self.dataset_root
    }

    pub(crate) fn blockchain(&self) -> &BlockchainId {
        &self.blockchain
    }
}

/// Store response data - uses untagged enum since JS sends flat JSON
/// Nack: {"errorMessage": "..."}
/// Ack: {"identityId": ..., "v": ..., "r": ..., "s": ..., "vs": ...}
///
/// Note: identity_id uses u64 instead of u128 because serde_json has issues
/// with u128 in untagged enums. This is safe since identity IDs won't exceed u64::MAX.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum StoreResponseData {
    #[serde(rename_all = "camelCase")]
    Nack { error_message: String },
    #[serde(rename_all = "camelCase")]
    Ack {
        identity_id: u64,
        #[serde(flatten)]
        signature: SignatureComponents,
    },
}

impl StoreResponseData {
    pub(crate) fn nack(message: impl Into<String>) -> Self {
        Self::Nack {
            error_message: message.into(),
        }
    }
}
