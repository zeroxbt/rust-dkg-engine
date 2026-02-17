//! Network manager module.
//!
//! This module provides networking capabilities for the node, including:
//! - Peer discovery via Kademlia DHT
//! - Request-response protocols for store, get, finality, and batch_get operations
//! - Connection management

mod actions;
mod behaviour;
mod config;
mod error;
mod event_loop;
mod handle;
mod handler;
mod key_manager;
mod message;
mod pending_requests;
mod protocols;
mod request_outcome;

pub use config::NetworkManagerConfig;
pub use error::NetworkError;
pub use event_loop::NetworkEventLoop;
pub use handle::NetworkManager;
pub use handler::{
    ImmediateResponse, InboundDecision, InboundRequest, NetworkEventHandler, ResponseHandle,
};
pub use key_manager::KeyManager;
// Re-export libp2p types for application use
pub use libp2p::identity::Keypair;
pub use libp2p::{Multiaddr, PeerId, StreamProtocol};
pub use protocols::{
    BatchGetAck, BatchGetRequestData, BatchGetResponseData, FinalityAck, FinalityRequestData,
    FinalityResponseData, GetAck, GetRequestData, GetResponseData, StoreAck, StoreRequestData,
    StoreResponseData,
};
pub use request_outcome::{IdentifyInfo, PeerEvent, RequestOutcome, RequestOutcomeKind};

// Stable protocol identifiers for application-layer routing/filtering.
pub const PROTOCOL_NAME_STORE: &str = <protocols::StoreProtocol as protocols::ProtocolSpec>::NAME;
pub const PROTOCOL_NAME_GET: &str = <protocols::GetProtocol as protocols::ProtocolSpec>::NAME;
pub const PROTOCOL_NAME_FINALITY: &str =
    <protocols::FinalityProtocol as protocols::ProtocolSpec>::NAME;
pub const PROTOCOL_NAME_BATCH_GET: &str =
    <protocols::BatchGetProtocol as protocols::ProtocolSpec>::NAME;

pub const STREAM_PROTOCOL_STORE: &str =
    <protocols::StoreProtocol as protocols::ProtocolSpec>::STREAM_PROTOCOL;
pub const STREAM_PROTOCOL_GET: &str =
    <protocols::GetProtocol as protocols::ProtocolSpec>::STREAM_PROTOCOL;
pub const STREAM_PROTOCOL_FINALITY: &str =
    <protocols::FinalityProtocol as protocols::ProtocolSpec>::STREAM_PROTOCOL;
pub const STREAM_PROTOCOL_BATCH_GET: &str =
    <protocols::BatchGetProtocol as protocols::ProtocolSpec>::STREAM_PROTOCOL;
