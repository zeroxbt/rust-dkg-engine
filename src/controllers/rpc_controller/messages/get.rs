use serde::{Deserialize, Serialize};

use crate::types::models::Assertion;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenIds {
    pub start_token_id: u64,
    pub end_token_id: u64,
    pub burned: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetRequestData {
    pub ual: String,
    pub token_ids: TokenIds,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GetResponseData {
    #[serde(rename_all = "camelCase")]
    Error {
        error_message: String,
    },
    Data {
        data: Assertion,
    },
}
