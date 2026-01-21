use serde::Serialize;
use triple_store::{Assertion, Visibility};
use uuid::Uuid;
use validator_derive::Validate;

#[derive(serde::Deserialize, Debug, Validate)]
#[serde(rename_all = "camelCase")]
pub struct GetRequest {
    /// The UAL (Universal Asset Locator) of the knowledge asset/collection to retrieve
    pub id: String,

    /// Whether to include metadata in the response
    #[serde(default)]
    pub include_metadata: bool,

    /// Optional paranet UAL for permissioned access
    #[serde(rename = "paranetUAL")]
    pub paranet_ual: Option<String>,

    /// Visibility filter: public, private, or all
    #[serde(default)]
    pub content_type: Visibility,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetResponse {
    pub operation_id: Uuid,
}

impl GetResponse {
    pub fn new(operation_id: Uuid) -> Self {
        Self { operation_id }
    }
}

/// Data specific to get operation results
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetOperationData {
    pub assertion: Assertion,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

/// Response for get operation result endpoint
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetOperationResultResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<GetOperationData>,
}

impl GetOperationResultResponse {
    pub fn in_progress() -> Self {
        Self {
            status: "IN_PROGRESS".to_string(),
            data: None,
        }
    }

    pub fn completed(assertion: Assertion, metadata: Option<Vec<String>>) -> Self {
        Self {
            status: "COMPLETED".to_string(),
            data: Some(GetOperationData {
                assertion,
                metadata,
                error_type: None,
                error_message: None,
            }),
        }
    }

    pub fn failed(error_message: Option<String>) -> Self {
        Self {
            status: "FAILED".to_string(),
            data: Some(GetOperationData {
                assertion: Assertion::default(),
                metadata: None,
                error_type: Some("OPERATION_FAILED".to_string()),
                error_message,
            }),
        }
    }
}
