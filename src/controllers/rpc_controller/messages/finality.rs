use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FinalityRequestData {
    pub ual: String,
    pub publish_operation_id: String,
}

impl FinalityRequestData {
    pub fn new(ual: String, publish_operation_id: String) -> Self {
        Self {
            ual,
            publish_operation_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FinalityResponseData {
    #[serde(rename_all = "camelCase")]
    Ack { message: String },
    #[serde(rename_all = "camelCase")]
    Nack { error_message: String },
}
