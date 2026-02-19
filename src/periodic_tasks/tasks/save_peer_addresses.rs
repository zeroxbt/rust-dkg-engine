use std::{sync::Arc, time::Duration};

use dkg_key_value_store::PeerAddressStore;
use tokio_util::sync::CancellationToken;

use crate::{
    node_state::PeerRegistry, periodic_tasks::SavePeerAddressesDeps,
    periodic_tasks::runner::run_with_shutdown,
};

const SAVE_PEER_ADDRESSES_PERIOD: Duration = Duration::from_secs(60);

pub(crate) struct SavePeerAddressesTask {
    peer_registry: Arc<PeerRegistry>,
    address_store: Arc<PeerAddressStore>,
}

impl SavePeerAddressesTask {
    pub(crate) fn new(deps: SavePeerAddressesDeps) -> Self {
        Self {
            peer_registry: deps.peer_registry,
            address_store: deps.peer_address_store,
        }
    }

    pub(crate) async fn run(self, shutdown: CancellationToken) {
        run_with_shutdown("save_peer_addresses", shutdown, || self.execute()).await;
    }

    #[tracing::instrument(
        name = "periodic_tasks.save_peer_addresses",
        skip(self),
        fields(
            peers = tracing::field::Empty,
        )
    )]
    async fn execute(&self) -> Duration {
        let addresses = self.peer_registry.get_all_addresses();

        tracing::Span::current().record("peers", addresses.len());

        if !addresses.is_empty() {
            let persisted_addresses = addresses
                .into_iter()
                .map(|(peer_id, addrs)| {
                    (
                        peer_id.to_string(),
                        addrs.into_iter().map(|addr| addr.to_string()).collect(),
                    )
                })
                .collect();
            self.address_store.save_all(&persisted_addresses).await;
        }

        SAVE_PEER_ADDRESSES_PERIOD
    }
}
