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
pub struct GetRequestData {
    assertion_id: String,
    blockchain: String,
    contract: String,
    token_id: u64,
    keyword: String,
    state: String,
    hash_function_id: u8,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetResponseData {
    error_message: Option<ErrorMessage>,
    nquads: Vec<String>,
}

impl GetResponseData {
    pub fn new(nquads: Vec<String>, error_message: Option<ErrorMessage>) -> Self {
        Self {
            error_message,
            nquads,
        }
    }
}
