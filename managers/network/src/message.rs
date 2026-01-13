use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Network-level error types for transport and protocol errors
#[derive(Debug, Serialize, Deserialize)]
pub enum ErrorMessage {
    Timeout,
    InvalidData,
    Custom(String),
}

/// Request message types for protocol negotiation
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum RequestMessageType {
    ProtocolRequest,
}

/// Response message types for flow control
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum ResponseMessageType {
    Ack,
    Nack,
    Busy,
}

/// Request message header containing operation tracking and message type
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RequestMessageHeader {
    pub operation_id: Uuid,
    pub message_type: RequestMessageType,
}

/// Response message header containing operation tracking and message type
#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseMessageHeader {
    pub operation_id: Uuid,
    pub message_type: ResponseMessageType,
}

/// Generic request message envelope
/// The generic parameter T should be an application-defined request data type
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RequestMessage<T> {
    pub header: RequestMessageHeader,
    pub data: T,
}

/// Generic response message envelope
/// The generic parameter T should be an application-defined response data type
#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseMessage<T> {
    pub header: ResponseMessageHeader,
    pub data: T,
}
