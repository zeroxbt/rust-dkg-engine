//! Protocol specification trait for request-response protocols.
//!
//! This module defines the `ProtocolSpec` trait which provides a type-safe way
//! to define protocol configurations. Each protocol (store, get, finality, batch_get)
//! implements this trait to specify its types and timeouts.

use std::time::Duration;

use serde::{Serialize, de::DeserializeOwned};

use crate::managers::network::message::ResponseBody;

/// Defines a request-response protocol's types and configuration.
///
/// This trait is intentionally decoupled from `NodeBehaviour` to avoid module cycles.
/// It provides compile-time configuration for protocol timeouts and associated types.
///
/// # Example
///
/// ```ignore
/// pub(crate) struct StoreProtocol;
///
/// impl ProtocolSpec for StoreProtocol {
///     const NAME: &'static str = "Store";
///     const STREAM_PROTOCOL: &'static str = "/store/1.0.0";
///     const TIMEOUT: Duration = Duration::from_secs(15);
///
///     type RequestData = StoreRequestData;
///     type Ack = StoreAck;
/// }
/// ```
pub(crate) trait ProtocolSpec: Send + Sync + 'static {
    /// Protocol name for logging and debugging.
    const NAME: &'static str;

    /// Stream protocol identifier (e.g., "/store/1.0.0").
    const STREAM_PROTOCOL: &'static str;

    /// Request timeout duration.
    const TIMEOUT: Duration;

    /// Request data type sent to peers.
    type RequestData: Serialize + DeserializeOwned + Send + Clone;

    /// ACK payload type received in successful responses.
    type Ack: Serialize + DeserializeOwned + Send + Clone;
}

/// Type alias for what callers receive from `send_*_request` methods.
///
/// This is `ResponseBody<Ack>` which can be either `Ack(T)` or `Error(ErrorPayload)`.
pub(crate) type ProtocolResponse<P> = ResponseBody<<P as ProtocolSpec>::Ack>;
