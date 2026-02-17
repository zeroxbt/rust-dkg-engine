//! Finality protocol implementation.
//!
//! The finality protocol is used to check the finality status of published
//! knowledge assets across network peers.

mod messages;

use std::time::Duration;

use super::ProtocolSpec;
pub use messages::{FinalityAck, FinalityRequestData, FinalityResponseData};

/// Finality protocol marker type.
pub struct FinalityProtocol;

impl ProtocolSpec for FinalityProtocol {
    const NAME: &'static str = "Finality";
    const STREAM_PROTOCOL: &'static str = "/finality/1.0.0";
    const TIMEOUT: Duration = Duration::from_secs(10);

    type RequestData = FinalityRequestData;
    type Ack = FinalityAck;
}
