use std::sync::Arc;

use dkg_network::{BatchGetAck, FinalityAck, GetAck, StoreAck};

use crate::state::ResponseChannels;

pub(crate) struct ProtocolResponseChannels {
    pub(crate) store: Arc<ResponseChannels<StoreAck>>,
    pub(crate) get: Arc<ResponseChannels<GetAck>>,
    pub(crate) finality: Arc<ResponseChannels<FinalityAck>>,
    pub(crate) batch_get: Arc<ResponseChannels<BatchGetAck>>,
}

impl ProtocolResponseChannels {
    pub(crate) fn new() -> Self {
        Self {
            store: Arc::new(ResponseChannels::new()),
            get: Arc::new(ResponseChannels::new()),
            finality: Arc::new(ResponseChannels::new()),
            batch_get: Arc::new(ResponseChannels::new()),
        }
    }
}
