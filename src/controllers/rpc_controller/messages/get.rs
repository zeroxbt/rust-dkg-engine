use serde::{Deserialize, Serialize};
use triple_store::{Assertion, TokenIds, Visibility};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetRequestData {
    ual: String,
    token_ids: TokenIds,
    include_metadata: bool,
    paranet_ual: Option<String>,
    content_type: Visibility,
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

    /// Returns the UAL.
    pub fn ual(&self) -> &str {
        &self.ual
    }

    /// Returns the token IDs.
    pub fn token_ids(&self) -> &TokenIds {
        &self.token_ids
    }

    /// Returns whether metadata should be included.
    pub fn include_metadata(&self) -> bool {
        self.include_metadata
    }

    /// Returns the paranet UAL, if any.
    pub fn paranet_ual(&self) -> Option<&str> {
        self.paranet_ual.as_deref()
    }

    /// Returns the content type (visibility).
    pub fn content_type(&self) -> Visibility {
        self.content_type
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
