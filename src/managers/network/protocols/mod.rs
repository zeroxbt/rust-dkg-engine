//! Network protocols module containing application-specific libp2p protocols.
//!
//! This module defines the protocol behaviours, codecs, and timeouts used by
//! the NetworkManager for peer-to-peer communication.

mod behaviour;
mod constants;
mod js_compat_codec;

pub(crate) use behaviour::{NetworkProtocols, NetworkProtocolsEvent};
pub(crate) use constants::ProtocolTimeouts;
pub(crate) use js_compat_codec::JsCompatCodec;
