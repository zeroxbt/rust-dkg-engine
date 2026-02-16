//! Store protocol message types.

use dkg_domain::{BlockchainId, SignatureComponents};
use serde::{Deserialize, Serialize};

use crate::message::ResponseBody;

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

/// Store ACK payload.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StoreAck {
    pub identity_id: u128,
    #[serde(flatten)]
    pub signature: SignatureComponents,
}

/// Store response data (ACK payload or error payload).
pub type StoreResponseData = ResponseBody<StoreAck>;
