//! Protocol message types for network communication.
//!
//! These types define the request/response formats for each protocol
//! (Store, Get, BatchGet, Finality).

mod batch_get;
mod finality;
mod get;
mod store;

pub(crate) use batch_get::{BatchGetRequestData, BatchGetResponseData};
pub(crate) use finality::{FinalityRequestData, FinalityResponseData};
pub(crate) use get::{GetRequestData, GetResponseData};
pub(crate) use store::{StoreRequestData, StoreResponseData};

#[cfg(test)]
mod tests;
