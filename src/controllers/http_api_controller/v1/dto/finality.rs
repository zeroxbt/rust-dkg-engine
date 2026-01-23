use serde::{Deserialize, Serialize};
use validator_derive::Validate;

use crate::controllers::http_api_controller::validators::validate_ual_format;

#[derive(Debug, Deserialize, Validate)]
pub(crate) struct FinalityRequest {
    #[validate(custom(function = "validate_ual_format"))]
    pub ual: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct FinalityStatusResponse {
    pub finality: u64,
}

impl FinalityStatusResponse {
    pub(crate) fn new(finality: u64) -> Self {
        Self { finality }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct FinalityStatusErrorResponse {
    pub message: String,
}

impl FinalityStatusErrorResponse {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
