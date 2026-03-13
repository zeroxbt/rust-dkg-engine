use std::sync::Arc;

use dkg_key_value_store::PeerAddressStore;

use crate::peer_registry::PeerRegistry;

#[derive(Clone)]
pub(crate) struct SavePeerAddressesDeps {
    pub(crate) peer_registry: Arc<PeerRegistry>,
    pub(crate) peer_address_store: Arc<PeerAddressStore>,
}
