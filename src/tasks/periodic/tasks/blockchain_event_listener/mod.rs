//! Blockchain event listener task.
//!
//! Polls blockchain networks for contract events and dispatches them to appropriate handlers.

mod task;

pub(crate) use task::BlockchainEventListenerTask;
