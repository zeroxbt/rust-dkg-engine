use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct FinalityRequest {
    pub ual: String,
}

#[derive(Debug, Serialize)]
pub struct FinalityStatusResponse {
    pub finality: u64,
}

impl FinalityStatusResponse {
    pub fn new(finality: u64) -> Self {
        Self { finality }
    }
}

#[derive(Debug, Serialize)]
pub struct FinalityStatusErrorResponse {
    pub message: String,
}

impl FinalityStatusErrorResponse {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
