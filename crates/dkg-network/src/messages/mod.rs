//! Protocol message types for network communication.
//!
//! **DEPRECATED**: This module is kept for backwards compatibility during
//! the migration to `protocols/`. New code should import from `protocols::*`.
//!
//! These types are now re-exported from the protocol submodules.

// Re-export from protocols for backwards compatibility
pub use super::protocols::{
    BatchGetAck, BatchGetRequestData, BatchGetResponseData, FinalityAck, FinalityRequestData,
    GetAck, GetRequestData, GetResponseData, StoreAck, StoreRequestData, StoreResponseData,
};

#[cfg(test)]
mod tests;
