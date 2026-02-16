//! Batch get protocol implementation.
//!
//! The batch get protocol is used to retrieve multiple knowledge assets
//! from network peers in a single request, improving efficiency for
//! bulk operations like sync.

mod messages;

use std::time::Duration;

use super::ProtocolSpec;
pub use crate::protocols::batch_get::messages::{
    BatchGetAck, BatchGetRequestData, BatchGetResponseData,
};

/// Batch get protocol marker type.
pub struct BatchGetProtocol;

impl ProtocolSpec for BatchGetProtocol {
    const NAME: &'static str = "BatchGet";
    const STREAM_PROTOCOL: &'static str = "/batch-get/1.0.0";
    const TIMEOUT: Duration = Duration::from_secs(10);

    type RequestData = BatchGetRequestData;
    type Ack = BatchGetAck;
}
