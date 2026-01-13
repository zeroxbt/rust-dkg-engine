use network::ErrorMessage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StoreRequestData {
    dataset: Vec<String>,
    dataset_root: String,
    blockchain: String,
}

impl StoreRequestData {
    pub fn new(dataset: Vec<String>, dataset_root: String, blockchain: String) -> Self {
        Self {
            dataset,
            dataset_root,
            blockchain,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoreResponseData {
    identity_id: String,
    v: u64,
    r: String,
    s: String,
    vs: String,
    error_message: Option<ErrorMessage>,
}

impl StoreResponseData {
    pub fn new(
        identity_id: String,
        v: u64,
        r: String,
        s: String,
        vs: String,
        error_message: Option<ErrorMessage>,
    ) -> Self {
        Self {
            identity_id,
            v,
            r,
            s,
            vs,
            error_message,
        }
    }
}
