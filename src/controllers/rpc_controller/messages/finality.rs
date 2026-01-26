use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FinalityRequestData {
    ual: String,
    publish_operation_id: String,
}

impl FinalityRequestData {
    pub(crate) fn new(ual: String, publish_operation_id: String) -> Self {
        Self {
            ual,
            publish_operation_id,
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
}

/// Finality response data - uses untagged enum since JS sends flat JSON
/// ACK: {"message": "..."}
/// NACK: {"errorMessage": "..."}
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum FinalityResponseData {
    #[serde(rename_all = "camelCase")]
    Ack { message: String },
    #[serde(rename_all = "camelCase")]
    Nack { error_message: String },
}
