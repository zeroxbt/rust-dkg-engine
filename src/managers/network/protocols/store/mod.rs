//! Store protocol implementation.
//!
//! The store protocol is used during publish operations to distribute
//! dataset chunks to network peers for storage.

mod messages;

pub(crate) use messages::*;

use std::time::Duration;

use super::ProtocolSpec;

/// Store protocol marker type.
pub(crate) struct StoreProtocol;

impl ProtocolSpec for StoreProtocol {
    const NAME: &'static str = "Store";
    const STREAM_PROTOCOL: &'static str = "/store/1.0.0";
    const TIMEOUT: Duration = Duration::from_secs(15);

    type RequestData = StoreRequestData;
    type Ack = StoreAck;
}
