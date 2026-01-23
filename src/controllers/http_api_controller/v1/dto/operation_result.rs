use serde::Serialize;

/// Signature data returned in the operation result
#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SignatureData {
    pub identity_id: String,
    pub v: u8,
    pub r: String,
    pub s: String,
    pub vs: String,
}

/// Data specific to publish operation results
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishOperationData {
    pub min_acks_reached: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publisher_node_signature: Option<SignatureData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signatures: Option<Vec<SignatureData>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

/// Response for operation result endpoint
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationResultResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<PublishOperationData>,
}

impl OperationResultResponse {
    pub fn pending() -> Self {
        Self {
            status: "PENDING".to_string(),
            data: None,
        }
    }

    pub fn in_progress() -> Self {
        Self {
            status: "IN_PROGRESS".to_string(),
            data: None,
        }
    }

    pub fn completed_with_signatures(
        publisher_signature: Option<SignatureData>,
        network_signatures: Vec<SignatureData>,
    ) -> Self {
        Self {
            status: "COMPLETED".to_string(),
            data: Some(PublishOperationData {
                min_acks_reached: true,
                publisher_node_signature: publisher_signature,
                signatures: Some(network_signatures),
                error_type: None,
                error_message: None,
            }),
        }
    }

    pub fn failed(error_message: Option<String>) -> Self {
        Self {
            status: "FAILED".to_string(),
            data: Some(PublishOperationData {
                min_acks_reached: false,
                publisher_node_signature: None,
                signatures: None,
                error_type: Some("OPERATION_FAILED".to_string()),
                error_message,
            }),
        }
    }
}

/// Error response for operation result endpoint
#[derive(Debug, Serialize)]
pub struct OperationResultErrorResponse {
    pub code: u16,
    pub message: String,
}

impl OperationResultErrorResponse {
    pub fn new(code: u16, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}
