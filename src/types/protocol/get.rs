use network::ErrorMessage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenIds {
    pub start_token_id: u64,
    pub end_token_id: u64,
    pub burned: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetRequestData {
    pub ual: String,
    pub token_ids: TokenIds,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Assertion {
    pub public: Vec<String>,
    pub private: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetResponseData {
    error_message: Option<ErrorMessage>,
    data: Option<Assertion>,
}

impl GetResponseData {
    pub fn new(error_message: Option<ErrorMessage>, data: Option<Assertion>) -> Self {
        Self {
            error_message,
            data,
        }
    }
}
