//! Network manager module.
//!
//! This module provides networking capabilities for the node, including:
//! - Peer discovery via Kademlia DHT
//! - Request-response protocols for store, get, finality, and batch_get operations
//! - Connection management

mod actions;
mod behaviour;
mod config;
pub mod error;
pub mod event_loop;
mod handle;
mod handler;
mod key_manager;
pub mod message;
mod pending_requests;
pub mod protocols;
mod request_outcome;

// Keep messages module for backwards compatibility during transition
// TODO: Remove this module after all imports are updated
pub mod messages;

pub use config::NetworkManagerConfig;
pub use error::NetworkError;
pub use event_loop::NetworkEventLoop;
pub use handle::NetworkManager;
pub use handler::{ImmediateResponse, InboundDecision, NetworkEventHandler};
pub use key_manager::KeyManager;
// Re-export libp2p types for application use
pub use libp2p::{PeerId, request_response};
// Re-export message types
pub use message::{RequestMessage, ResponseMessage};
pub use pending_requests::{PendingRequests, RequestContext};
pub use protocols::JsCompatCodec;
pub use request_outcome::{IdentifyInfo, PeerEvent, RequestOutcome, RequestOutcomeKind};
