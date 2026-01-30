use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

/// Request message types for protocol negotiation
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum RequestMessageType {
    #[serde(rename = "PROTOCOL_REQUEST")]
    ProtocolRequest,
}

/// Response message types for flow control
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub(crate) enum ResponseMessageType {
    #[serde(rename = "ACK")]
    Ack,
    #[serde(rename = "NACK")]
    Nack,
    #[serde(rename = "BUSY")]
    Busy,
}

/// Request message header containing operation tracking and message type
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RequestMessageHeader {
    operation_id: Uuid,
    message_type: RequestMessageType,
}

impl RequestMessageHeader {
    /// Creates a new request message header.
    pub(crate) fn new(operation_id: Uuid, message_type: RequestMessageType) -> Self {
        Self {
            operation_id,
            message_type,
        }
    }

    /// Returns the operation ID.
    pub(crate) fn operation_id(&self) -> Uuid {
        self.operation_id
    }
}

/// Response message header containing operation tracking and message type
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResponseMessageHeader {
    operation_id: Uuid,
    message_type: ResponseMessageType,
}

impl ResponseMessageHeader {
    /// Creates a new response message header.
    pub(crate) fn new(operation_id: Uuid, message_type: ResponseMessageType) -> Self {
        Self {
            operation_id,
            message_type,
        }
    }
}

/// Generic request message envelope
/// The generic parameter T should be an application-defined request data type
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct RequestMessage<T> {
    pub header: RequestMessageHeader,
    pub data: T,
}

/// Error payload for NACK/BUSY responses.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ErrorPayload {
    #[serde(default)]
    pub error_message: String,
}

impl ErrorPayload {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            error_message: message.into(),
        }
    }
}

/// Response body that depends on the response header.
#[derive(Debug, Serialize, Clone)]
#[serde(untagged)]
pub(crate) enum ResponseBody<T> {
    Ack(T),
    Error(ErrorPayload),
}

impl<T> ResponseBody<T> {
    pub(crate) fn ack(data: T) -> Self {
        Self::Ack(data)
    }

    pub(crate) fn error(message: impl Into<String>) -> Self {
        Self::Error(ErrorPayload::new(message))
    }
}

/// Generic response message envelope
/// The generic parameter T should be an application-defined response data type
#[derive(Debug, Serialize, Clone)]
pub(crate) struct ResponseMessage<T> {
    pub header: ResponseMessageHeader,
    pub data: ResponseBody<T>,
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

        let header: ResponseMessageHeader =
            serde_json::from_value(header_value).map_err(serde::de::Error::custom)?;

        let data = match header.message_type {
            ResponseMessageType::Ack => {
                let ack = serde_json::from_value::<T>(data_value)
                    .map_err(serde::de::Error::custom)?;
                ResponseBody::Ack(ack)
            }
            ResponseMessageType::Nack | ResponseMessageType::Busy => {
                let error = serde_json::from_value::<ErrorPayload>(data_value)
                    .map_err(serde::de::Error::custom)?;
                ResponseBody::Error(error)
            }
        };

        Ok(Self { header, data })
    }
}
