//! Finality protocol message types.

use dkg_domain::BlockchainId;
use serde::{Deserialize, Serialize};

use crate::message::ResponseBody;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FinalityRequestData {
    ual: String,
    publish_operation_id: String,
    blockchain: BlockchainId,
}

impl FinalityRequestData {
    pub fn new(ual: String, publish_operation_id: String, blockchain: BlockchainId) -> Self {
        Self {
            ual,
            publish_operation_id,
            blockchain,
        }
    }

    /// Returns the UAL.
    pub fn ual(&self) -> &str {
        &self.ual
    }

    /// Returns the publish operation ID.
    pub fn publish_operation_id(&self) -> &str {
        &self.publish_operation_id
    }
}

/// Finality ACK payload.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FinalityAck {
    pub message: String,
}

/// Finality response data (ACK payload or error payload).
pub type FinalityResponseData = ResponseBody<FinalityAck>;
