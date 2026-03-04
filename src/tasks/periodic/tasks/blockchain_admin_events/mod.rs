//! Blockchain admin events task.
//!
//! Polls blockchain networks for non-KC admin/configuration events and dispatches them to handlers.

mod deps;
mod config;
mod task;

pub(crate) use config::BlockchainAdminEventsConfig;
pub(crate) use deps::BlockchainAdminEventsDeps;
pub(crate) use task::BlockchainAdminEventsTask;
