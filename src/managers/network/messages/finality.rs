use serde::{Deserialize, Serialize};

use crate::{managers::network::message::ResponseBody, types::BlockchainId};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FinalityRequestData {
    ual: String,
    publish_operation_id: String,
    blockchain: BlockchainId,
}

impl FinalityRequestData {
    pub(crate) fn new(ual: String, publish_operation_id: String, blockchain: BlockchainId) -> Self {
        Self {
            ual,
            publish_operation_id,
            blockchain,
        }
    }

    /// Returns the UAL.
    pub(crate) fn ual(&self) -> &str {
        &self.ual
    }

    /// Returns the publish operation ID.
    pub(crate) fn publish_operation_id(&self) -> &str {
        &self.publish_operation_id
    }

    /// Returns the blockchain identifier.
    pub(crate) fn blockchain(&self) -> &BlockchainId {
        &self.blockchain
    }
}

/// Finality ACK payload.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FinalityAck {
    pub message: String,
}

/// Finality response data (ACK payload or error payload).
pub(crate) type FinalityResponseData = ResponseBody<FinalityAck>;
