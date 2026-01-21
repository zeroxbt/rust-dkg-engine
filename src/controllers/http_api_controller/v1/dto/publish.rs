use blockchain::BlockchainId;
use serde::{Deserialize, Serialize};
use triple_store::Assertion;
use uuid::Uuid;
use validator::{Validate as _, ValidationError};
use validator_derive::Validate;

use crate::controllers::http_api_controller::validators::validate_blockchain_id_format;

/// Validates BlockchainId format at the DTO level.
fn validate_blockchain_id(id: &BlockchainId) -> Result<(), ValidationError> {
    validate_blockchain_id_format(id.as_str())
}

#[derive(Deserialize, Debug, Validate)]
#[serde(rename_all = "camelCase")]
pub struct PublishRequest {
    #[validate(length(
        equal = 66,
        message = "dataset_root must be exactly 66 characters (0x + 64 hex chars)"
    ))]
    pub dataset_root: String,
    #[validate(nested)]
    pub dataset: Assertion,
    #[validate(custom(function = "validate_blockchain_id"))]
    pub blockchain: BlockchainId,
    #[validate(range(min = 1, message = "hash_function_id must be at least 1"))]
    pub hash_function_id: Option<u8>,
    #[validate(range(
        min = 1,
        message = "minimum_number_of_node_replications must be at least 1"
    ))]
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
