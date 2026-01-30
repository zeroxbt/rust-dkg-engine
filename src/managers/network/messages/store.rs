use serde::{Deserialize, Serialize};

use crate::{
    managers::network::message::ResponseBody,
    types::{BlockchainId, SignatureComponents},
};

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

/// Store ACK payload.
///
/// Note: identity_id uses u64 instead of u128 because serde_json has issues
/// with u128 in untagged enums. This is safe since identity IDs won't exceed u64::MAX.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StoreAck {
    pub identity_id: u64,
    #[serde(flatten)]
    pub signature: SignatureComponents,
}

/// Store response data (ACK payload or error payload).
pub(crate) type StoreResponseData = ResponseBody<StoreAck>;
