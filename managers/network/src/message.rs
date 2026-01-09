use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub enum ErrorMessage {
    Timeout,
    InvalidData,
    Custom(String),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum RequestMessageType {
    ProtocolRequest,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum ResponseMessageType {
    Ack,
    Nack,
    Busy,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RequestMessageHeader {
    pub operation_id: Uuid,
    pub message_type: RequestMessageType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseMessageHeader {
    pub operation_id: Uuid,
    pub message_type: ResponseMessageType,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RequestMessage<T> {
    pub header: RequestMessageHeader,
    pub data: T,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseMessage<T> {
    pub header: ResponseMessageHeader,
    pub data: T,
}

// STORE

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StoreRequestData {
    dataset: Vec<String>,
    dataset_root: String,
    blockchain: String,
}

impl StoreRequestData {
    pub fn new(dataset: Vec<String>, dataset_root: String, blockchain: String) -> Self {
        Self {
            dataset,
            dataset_root,
            blockchain,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoreResponseData {
    identity_id: String,
    v: u64,
    r: String,
    s: String,
    vs: String,
    error_message: Option<ErrorMessage>,
}

impl StoreResponseData {
    pub fn new(
        identity_id: String,
        v: u64,
        r: String,
        s: String,
        vs: String,
        error_message: Option<ErrorMessage>,
    ) -> Self {
        Self {
            identity_id,
            v,
            r,
            s,
            vs,
            error_message,
        }
    }
}

// GET

#[derive(Debug, Serialize, Deserialize)]
struct TokenIds {
    start_token_id: u64,
    ent_token_id: u64,
    burned: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetRequestData {
    ual: String,
    token_ids: TokenIds,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Assertion {
    public: Vec<String>,
    private: Option<Vec<String>>,
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

// FINALITY

#[derive(Debug, Serialize, Deserialize)]
pub struct FinalityRequestData {
    ual: String,
    publish_operation_id: String,
    operation_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FinalityResponseData {}
