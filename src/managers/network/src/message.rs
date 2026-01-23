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
    operation_id: Uuid,
    message_type: RequestMessageType,
}

impl RequestMessageHeader {
    /// Creates a new request message header.
    pub fn new(operation_id: Uuid, message_type: RequestMessageType) -> Self {
        Self {
            operation_id,
            message_type,
        }
    }

    /// Returns the operation ID.
    pub fn operation_id(&self) -> Uuid {
        self.operation_id
    }

    /// Returns the message type.
    pub fn message_type(&self) -> &RequestMessageType {
        &self.message_type
    }
}

/// Response message header containing operation tracking and message type
#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseMessageHeader {
    operation_id: Uuid,
    message_type: ResponseMessageType,
}

impl ResponseMessageHeader {
    /// Creates a new response message header.
    pub fn new(operation_id: Uuid, message_type: ResponseMessageType) -> Self {
        Self {
            operation_id,
            message_type,
        }
    }

    /// Returns the operation ID.
    pub fn operation_id(&self) -> Uuid {
        self.operation_id
    }

    /// Returns the message type.
    pub fn message_type(&self) -> &ResponseMessageType {
        &self.message_type
    }
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
