//! Get protocol implementation.
//!
//! The get protocol is used to retrieve knowledge assets from network peers.

mod messages;

pub(crate) use messages::*;

use std::time::Duration;

use super::ProtocolSpec;

/// Get protocol marker type.
pub(crate) struct GetProtocol;

impl ProtocolSpec for GetProtocol {
    const NAME: &'static str = "Get";
    const STREAM_PROTOCOL: &'static str = "/get/1.0.0";
    const TIMEOUT: Duration = Duration::from_secs(15);

    type RequestData = GetRequestData;
    type Ack = GetAck;
}
