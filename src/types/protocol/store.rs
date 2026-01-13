use blockchain::BlockchainName;
use network::ErrorMessage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StoreRequestData {
    dataset: Vec<String>,
    dataset_root: String,
    blockchain: BlockchainName,
}

impl StoreRequestData {
    pub fn new(dataset: Vec<String>, dataset_root: String, blockchain: BlockchainName) -> Self {
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

    pub fn blockchain(&self) -> BlockchainName {
        self.blockchain
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StoreResponseData {
    Error {
        error_message: String,
    },
    Data {
        identity_id: String,
        v: u64,
        r: String,
        s: String,
        vs: String,
    },
}
