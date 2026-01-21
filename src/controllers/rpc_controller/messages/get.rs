use serde::{Deserialize, Serialize};
use triple_store::{Assertion, TokenIds, Visibility};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetRequestData {
    pub ual: String,
    pub token_ids: TokenIds,
    pub include_metadata: bool,
    pub paranet_ual: Option<String>,
    pub content_type: Visibility,
}

impl GetRequestData {
    pub fn new(
        ual: String,
        token_ids: TokenIds,
        include_metadata: bool,
        paranet_ual: Option<String>,
        content_type: Visibility,
    ) -> Self {
        Self {
            ual,
            token_ids,
            include_metadata,
            paranet_ual,
            content_type,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GetResponseData {
    #[serde(rename_all = "camelCase")]
    Error { error_message: String },
    Data {
        assertion: Assertion,
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<Vec<String>>,
    },
}

impl GetResponseData {
    pub fn data(assertion: Assertion, metadata: Option<Vec<String>>) -> Self {
        Self::Data {
            assertion,
            metadata,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self::Error {
            error_message: message.into(),
        }
    }
}
