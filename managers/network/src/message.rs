use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum ErrorMessage {
    Timeout,
    InvalidData,
    Custom(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RequestMessageType {
    ProtocolInit,
    ProtocolRequest,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ResponseMessageType {
    Ack,
    Nack,
    Busy,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestMessageHeader {
    pub operation_id: String,
    pub keyword_uuid: String,
    pub message_type: RequestMessageType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseMessageHeader {
    pub operation_id: String,
    pub keyword_uuid: String,
    pub message_type: ResponseMessageType,
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
pub struct StoreInitRequestData {
    assertion_id: String,
    blockchain: String,
    contract: String,
    token_id: u64,
    keyword: String,
    hash_function_id: u8,
}

impl StoreInitRequestData {
    pub fn new(
        assertion_id: String,
        blockchain: String,
        contract: String,
        token_id: u64,
        keyword: String,
        hash_function_id: u8,
    ) -> Self {
        Self {
            assertion_id,
            blockchain,
            contract,
            token_id,
            keyword,
            hash_function_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoreRequestData {
    assertion: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoreInitResponseData {
    error_message: Option<ErrorMessage>,
}

impl StoreInitResponseData {
    pub fn new(error_message: Option<ErrorMessage>) -> Self {
        Self { error_message }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoreRequestResponseData {
    error_message: Option<ErrorMessage>,
}

impl StoreRequestResponseData {
    pub fn new(error_message: Option<ErrorMessage>) -> Self {
        Self { error_message }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StoreMessageRequestData {
    Init(StoreInitRequestData),
    Request(StoreRequestData),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StoreMessageResponseData {
    InitResponse(StoreInitResponseData),
    RequestResponse(StoreRequestResponseData),
}

// GET

#[derive(Debug, Serialize, Deserialize)]
pub struct GetInitRequestData {
    assertion_id: String,
    blockchain: String,
    contract: String,
    token_id: u64,
    keyword: String,
    state: String,
}

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
pub struct GetInitResponseData {
    error_message: Option<ErrorMessage>,
}

impl GetInitResponseData {
    pub fn new(error_message: Option<ErrorMessage>) -> Self {
        Self { error_message }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetRequestResponseData {
    error_message: Option<ErrorMessage>,
    nquads: Vec<String>,
}

impl GetRequestResponseData {
    pub fn new(nquads: Vec<String>, error_message: Option<ErrorMessage>) -> Self {
        Self {
            error_message,
            nquads,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum GetMessageRequestData {
    Init(GetInitRequestData),
    Request(GetRequestData),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum GetMessageResponseData {
    InitResponse(GetInitResponseData),
    RequestResponse(GetRequestResponseData),
}
