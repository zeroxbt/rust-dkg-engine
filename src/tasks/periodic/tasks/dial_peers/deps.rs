use std::sync::Arc;

use dkg_network::NetworkManager;

use crate::peer_registry::PeerRegistry;

#[derive(Clone)]
pub(crate) struct DialPeersDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) peer_registry: Arc<PeerRegistry>,
}
