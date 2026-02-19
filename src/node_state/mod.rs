pub(crate) mod response_channels;

use std::sync::Arc;

use dkg_network::{BatchGetAck, FinalityAck, GetAck, StoreAck};

pub(crate) use dkg_peer_registry::PeerRegistry;
pub(crate) use response_channels::ResponseChannels;

pub(crate) struct NodeState {
    pub(crate) peer_registry: Arc<PeerRegistry>,
    pub(crate) store_response_channels: Arc<ResponseChannels<StoreAck>>,
    pub(crate) get_response_channels: Arc<ResponseChannels<GetAck>>,
    pub(crate) finality_response_channels: Arc<ResponseChannels<FinalityAck>>,
    pub(crate) batch_get_response_channels: Arc<ResponseChannels<BatchGetAck>>,
}

pub(crate) fn initialize() -> NodeState {
    NodeState {
        peer_registry: Arc::new(PeerRegistry::new()),
        store_response_channels: Arc::new(ResponseChannels::new()),
        get_response_channels: Arc::new(ResponseChannels::new()),
        finality_response_channels: Arc::new(ResponseChannels::new()),
        batch_get_response_channels: Arc::new(ResponseChannels::new()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn initialize_builds_distinct_response_channel_sets() {
        let state = initialize();

        assert_eq!(state.store_response_channels.cleanup_expired(), 0);
        assert_eq!(state.get_response_channels.cleanup_expired(), 0);
        assert_eq!(state.finality_response_channels.cleanup_expired(), 0);
        assert_eq!(state.batch_get_response_channels.cleanup_expired(), 0);

        let store_ptr = Arc::as_ptr(&state.store_response_channels) as *const ();
        let get_ptr = Arc::as_ptr(&state.get_response_channels) as *const ();
        let finality_ptr = Arc::as_ptr(&state.finality_response_channels) as *const ();
        let batch_get_ptr = Arc::as_ptr(&state.batch_get_response_channels) as *const ();

        assert_ne!(store_ptr, get_ptr);
        assert_ne!(store_ptr, finality_ptr);
        assert_ne!(store_ptr, batch_get_ptr);
        assert_ne!(get_ptr, finality_ptr);
        assert_ne!(get_ptr, batch_get_ptr);
        assert_ne!(finality_ptr, batch_get_ptr);
    }
}
