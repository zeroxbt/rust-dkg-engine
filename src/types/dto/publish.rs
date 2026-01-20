use blockchain::BlockchainId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator_derive::Validate;

use crate::types::models::Dataset;

#[derive(Deserialize, Debug, Validate)]
#[serde(rename_all = "camelCase")]
pub struct PublishRequest {
    #[validate(length(equal = 66))]
    pub dataset_root: String,
    pub dataset: Dataset,
    pub blockchain: BlockchainId,
    #[validate(range(min = 1))]
    pub hash_function_id: Option<u8>,
    #[validate(range(min = 1))]
    pub minimum_number_of_node_replications: Option<u8>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PublishResponse {
    pub operation_id: Uuid,
}

impl PublishResponse {
    pub fn new(operation_id: Uuid) -> Self {
        Self { operation_id }
    }
}
