pub(crate) mod peer_directory;
pub(crate) mod protocol_response_channels;

use std::sync::Arc;

pub(crate) use peer_directory::PeerDirectory;
pub(crate) use protocol_response_channels::ProtocolResponseChannels;

pub(crate) struct RuntimeState {
    pub(crate) peer_directory: Arc<PeerDirectory>,
    pub(crate) response_channels: Arc<ProtocolResponseChannels>,
}

pub(crate) fn initialize() -> RuntimeState {
    RuntimeState {
        peer_directory: Arc::new(PeerDirectory::new()),
        response_channels: Arc::new(ProtocolResponseChannels::new()),
    }
}
