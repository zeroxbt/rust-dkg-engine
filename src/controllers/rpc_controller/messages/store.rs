use serde::{Deserialize, Serialize};

use crate::managers::blockchain::{BlockchainId, SignatureComponents};

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

// TODO: This will not work for js implementation because of enum. I tried using #[serde(untagged)]
// to remove enum variant names from the json but it has deserialization issues and throws errors.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum StoreResponseData {
    #[serde(rename_all = "camelCase")]
    Error { error_message: String },
    #[serde(rename_all = "camelCase")]
    Data {
        identity_id: u128,
        #[serde(flatten)]
        signature: SignatureComponents,
    },
}
