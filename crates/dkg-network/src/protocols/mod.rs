//! Network protocols module containing application-specific libp2p protocols.
//!
//! This module defines the codecs and protocol specifications used by the
//! NetworkManager for peer-to-peer communication.
//!
//! Each protocol has its own submodule containing:
//! - Message types (request data, ACK payloads, response types)
//! - ProtocolSpec implementation with timeout configuration

mod codec;
mod spec;

// Protocol submodules
pub mod batch_get;
pub mod finality;
pub mod get;
pub mod store;

// Re-export protocol types for convenience
pub use batch_get::{BatchGetAck, BatchGetProtocol, BatchGetRequestData, BatchGetResponseData};
pub use codec::JsCompatCodec;
pub use finality::{FinalityAck, FinalityProtocol, FinalityRequestData, FinalityResponseData};
pub use get::{GetAck, GetProtocol, GetRequestData, GetResponseData};
pub use spec::{ProtocolResponse, ProtocolSpec};
pub use store::{StoreAck, StoreProtocol, StoreRequestData, StoreResponseData};
