//! Get protocol implementation.
//!
//! The get protocol is used to retrieve knowledge assets from network peers.

mod messages;

use std::time::Duration;

use super::ProtocolSpec;
pub use messages::{GetAck, GetRequestData, GetResponseData};

/// Get protocol marker type.
pub struct GetProtocol;

impl ProtocolSpec for GetProtocol {
    const NAME: &'static str = "Get";
    const STREAM_PROTOCOL: &'static str = "/get/1.0.0";
    const TIMEOUT: Duration = Duration::from_secs(5);

    type RequestData = GetRequestData;
    type Ack = GetAck;
}
