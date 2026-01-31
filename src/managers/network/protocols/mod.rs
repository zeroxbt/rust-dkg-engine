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
pub(crate) mod batch_get;
pub(crate) mod finality;
pub(crate) mod get;
pub(crate) mod store;

pub(crate) use codec::JsCompatCodec;
pub(crate) use spec::{ProtocolResponse, ProtocolSpec};

// Re-export protocol types for convenience
pub(crate) use batch_get::{BatchGetAck, BatchGetProtocol, BatchGetRequestData, BatchGetResponseData};
pub(crate) use finality::{FinalityAck, FinalityProtocol, FinalityRequestData, FinalityResponseData};
pub(crate) use get::{GetAck, GetProtocol, GetRequestData, GetResponseData};
pub(crate) use store::{StoreAck, StoreProtocol, StoreRequestData, StoreResponseData};
