//! Network manager module.
//!
//! This module provides networking capabilities for the node, including:
//! - Peer discovery via Kademlia DHT
//! - Request-response protocols for store, get, finality, and batch_get operations
//! - Connection management

mod actions;
mod behaviour;
mod config;
pub(crate) mod error;
pub(crate) mod event_loop;
mod handle;
mod handler;
mod key_manager;
pub(crate) mod message;
mod pending_requests;
pub(crate) mod protocols;

// Keep messages module for backwards compatibility during transition
// TODO: Remove this module after all imports are updated
pub(crate) mod messages;

pub(crate) use config::NetworkManagerConfig;
pub(crate) use error::NetworkError;
pub(crate) use event_loop::NetworkEventLoop;
pub(crate) use handle::NetworkManager;
pub(crate) use handler::NetworkEventHandler;
pub(crate) use key_manager::KeyManager;
// Re-export libp2p types for application use
pub(crate) use libp2p::{Multiaddr, PeerId, request_response};
// Re-export message types
pub(crate) use message::{RequestMessage, ResponseMessage};
pub(crate) use pending_requests::PendingRequests;
pub(crate) use protocols::JsCompatCodec;
