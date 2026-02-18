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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_initializes_all_channel_sets() {
        let channels = ProtocolResponseChannels::new();

        // Empty caches should have nothing to clean up.
        assert_eq!(channels.store.cleanup_expired(), 0);
        assert_eq!(channels.get.cleanup_expired(), 0);
        assert_eq!(channels.finality.cleanup_expired(), 0);
        assert_eq!(channels.batch_get.cleanup_expired(), 0);

        // Ensure each protocol uses a distinct cache instance.
        let store_ptr = Arc::as_ptr(&channels.store) as *const ();
        let get_ptr = Arc::as_ptr(&channels.get) as *const ();
        let finality_ptr = Arc::as_ptr(&channels.finality) as *const ();
        let batch_get_ptr = Arc::as_ptr(&channels.batch_get) as *const ();

        assert_ne!(store_ptr, get_ptr);
        assert_ne!(store_ptr, finality_ptr);
        assert_ne!(store_ptr, batch_get_ptr);
        assert_ne!(get_ptr, finality_ptr);
        assert_ne!(get_ptr, batch_get_ptr);
        assert_ne!(finality_ptr, batch_get_ptr);
    }
}
