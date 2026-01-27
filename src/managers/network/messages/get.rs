use serde::{Deserialize, Serialize};

use crate::types::{Assertion, TokenIds, Visibility};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GetRequestData {
    ual: String,
    token_ids: TokenIds,
    include_metadata: bool,
    paranet_ual: Option<String>,
    content_type: Visibility,
}

impl GetRequestData {
    pub(crate) fn new(
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
    pub(crate) fn ual(&self) -> &str {
        &self.ual
    }

    /// Returns the token IDs.
    pub(crate) fn token_ids(&self) -> &TokenIds {
        &self.token_ids
    }

    /// Returns whether metadata should be included.
    pub(crate) fn include_metadata(&self) -> bool {
        self.include_metadata
    }

    /// Returns the paranet UAL, if any.
    pub(crate) fn paranet_ual(&self) -> Option<&str> {
        self.paranet_ual.as_deref()
    }

    /// Returns the content type (visibility).
    pub(crate) fn content_type(&self) -> Visibility {
        self.content_type
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum GetResponseData {
    #[serde(rename_all = "camelCase")]
    Error { error_message: String },
    Data {
        assertion: Assertion,
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<Vec<String>>,
    },
}

impl GetResponseData {
    pub(crate) fn data(assertion: Assertion, metadata: Option<Vec<String>>) -> Self {
        Self::Data {
            assertion,
            metadata,
        }
    }

    pub(crate) fn error(message: impl Into<String>) -> Self {
        Self::Error {
            error_message: message.into(),
        }
    }
}
