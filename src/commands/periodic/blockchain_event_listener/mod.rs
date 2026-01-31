//! Blockchain event listener command.
//!
//! Polls blockchain networks for contract events and dispatches them to appropriate handlers.

mod blockchain_event_spec;
mod handler;

pub(crate) use handler::{
    BlockchainEventListenerCommandData, BlockchainEventListenerCommandHandler,
};
