use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use uuid::Uuid;

/// Wire-level message type used by request and response envelopes.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
pub enum MessageType {
    #[serde(rename = "PROTOCOL_REQUEST")]
    ProtocolRequest,
    #[serde(rename = "ACK")]
    Ack,
    #[serde(rename = "NACK")]
    Nack,
    #[serde(rename = "BUSY")]
    Busy,
}

/// Common wire header containing operation tracking and message type.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MessageHeader {
    operation_id: Uuid,
    message_type: MessageType,
}

impl MessageHeader {
    /// Creates a new wire message header.
    pub fn new(operation_id: Uuid, message_type: MessageType) -> Self {
        Self {
            operation_id,
            message_type,
        }
    }

    /// Returns the operation ID.
    pub fn operation_id(&self) -> Uuid {
        self.operation_id
    }

    /// Returns the wire-level message type.
    pub fn message_type(&self) -> MessageType {
        self.message_type
    }

    pub fn protocol_request(operation_id: Uuid) -> Self {
        Self::new(operation_id, MessageType::ProtocolRequest)
    }

    pub fn ack(operation_id: Uuid) -> Self {
        Self::new(operation_id, MessageType::Ack)
    }

    pub fn nack(operation_id: Uuid) -> Self {
        Self::new(operation_id, MessageType::Nack)
    }

    pub fn busy(operation_id: Uuid) -> Self {
        Self::new(operation_id, MessageType::Busy)
    }
}

/// Generic request message envelope
/// The generic parameter T should be an application-defined request data type
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RequestMessage<T> {
    pub header: MessageHeader,
    pub data: T,
}

impl<T> RequestMessage<T> {
    pub fn protocol_request(operation_id: Uuid, data: T) -> Self {
        Self {
            header: MessageHeader::protocol_request(operation_id),
            data,
        }
    }
}

/// Error payload for NACK/BUSY responses.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ErrorPayload {
    #[serde(default)]
    pub error_message: String,
}

impl ErrorPayload {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            error_message: message.into(),
        }
    }
}

/// Protocol-level response payload (ACK data or error payload).
#[derive(Debug, Serialize, Clone)]
#[serde(untagged)]
pub enum ProtocolResponse<T> {
    Ack(T),
    Error(ErrorPayload),
}

impl<T> ProtocolResponse<T> {
    pub fn ack(data: T) -> Self {
        Self::Ack(data)
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self::Error(ErrorPayload::new(message))
    }
}

/// Generic response message envelope
/// The generic parameter T should be an application-defined response data type
#[derive(Debug, Serialize, Clone)]
pub struct ResponseMessage<T> {
    pub header: MessageHeader,
    pub data: ProtocolResponse<T>,
}

impl<T> ResponseMessage<T> {
    pub(crate) fn ack(operation_id: Uuid, data: T) -> Self {
        Self {
            header: MessageHeader::ack(operation_id),
            data: ProtocolResponse::ack(data),
        }
    }

    pub(crate) fn nack(operation_id: Uuid, error_message: impl Into<String>) -> Self {
        Self {
            header: MessageHeader::nack(operation_id),
            data: ProtocolResponse::error(error_message),
        }
    }

    pub(crate) fn busy(operation_id: Uuid, error_message: impl Into<String>) -> Self {
        Self {
            header: MessageHeader::busy(operation_id),
            data: ProtocolResponse::error(error_message),
        }
    }
}

impl<'de, T> Deserialize<'de> for ResponseMessage<T>
where
    T: DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;

        let header_value = value
            .get("header")
            .ok_or_else(|| serde::de::Error::custom("missing header"))?
            .clone();
        let data_value = value.get("data").cloned().unwrap_or(Value::Null);

        let header: MessageHeader =
            serde_json::from_value(header_value).map_err(serde::de::Error::custom)?;

        let data = match header.message_type() {
            MessageType::Ack => {
                let ack =
                    serde_json::from_value::<T>(data_value).map_err(serde::de::Error::custom)?;
                ProtocolResponse::Ack(ack)
            }
            MessageType::Nack | MessageType::Busy => {
                let error = serde_json::from_value::<ErrorPayload>(data_value)
                    .map_err(serde::de::Error::custom)?;
                ProtocolResponse::Error(error)
            }
            MessageType::ProtocolRequest => {
                return Err(serde::de::Error::custom(
                    "invalid response message type: PROTOCOL_REQUEST",
                ));
            }
        };

        Ok(Self { header, data })
    }
}

#[cfg(test)]
#[path = "messages/tests.rs"]
mod tests;
