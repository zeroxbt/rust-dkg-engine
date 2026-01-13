use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct FinalityRequestData {
    pub ual: String,
    pub publish_operation_id: String,
    pub operation_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FinalityResponseData {}
