use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::{Validate as _, ValidationError};
use validator_derive::Validate;

use crate::{
    controllers::http_api_controller::validators::validate_blockchain_id_format,
    managers::blockchain::BlockchainId, types::Assertion,
};

/// Validates BlockchainId format at the DTO level.
fn validate_blockchain_id(id: &BlockchainId) -> Result<(), ValidationError> {
    validate_blockchain_id_format(id.as_str())
}

#[derive(Deserialize, Debug, Validate)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PublishRequest {
    #[validate(length(
        equal = 66,
        message = "dataset_root must be exactly 66 characters (0x + 64 hex chars)"
    ))]
    pub dataset_root: String,
    #[validate(nested)]
    pub dataset: Assertion,
    #[validate(custom(function = "validate_blockchain_id"))]
    pub blockchain: BlockchainId,
    #[validate(range(
        min = 1,
        message = "minimum_number_of_node_replications must be at least 1"
    ))]
    #[serde(default)]
    pub minimum_number_of_node_replications: u8,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PublishResponse {
    pub operation_id: Uuid,
}

impl PublishResponse {
    pub(crate) fn new(operation_id: Uuid) -> Self {
        Self { operation_id }
    }
}
