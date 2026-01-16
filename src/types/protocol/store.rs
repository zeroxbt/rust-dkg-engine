use blockchain::{BlockchainId, utils::SignatureComponents};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StoreRequestData {
    dataset: Vec<String>,
    dataset_root: String,
    blockchain: BlockchainId,
}

impl StoreRequestData {
    pub fn new(dataset: Vec<String>, dataset_root: String, blockchain: BlockchainId) -> Self {
        Self {
            dataset,
            dataset_root,
            blockchain,
        }
    }

    pub fn dataset(&self) -> &Vec<String> {
        &self.dataset
    }

    pub fn dataset_root(&self) -> &str {
        &self.dataset_root
    }

    pub fn blockchain(&self) -> &BlockchainId {
        &self.blockchain
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum StoreResponseData {
    #[serde(rename_all = "camelCase")]
    Error { error_message: String },
    #[serde(rename_all = "camelCase")]
    Data {
        identity_id: u128,
        #[serde(flatten)]
        signature: SignatureComponents,
    },
}
