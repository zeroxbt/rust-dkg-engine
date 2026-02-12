//! Blockchain event listener task.
//!
//! Polls blockchain networks for contract events and dispatches them to appropriate handlers.

mod blockchain_event_spec;
mod task;

pub(crate) use task::BlockchainEventListenerTask;
