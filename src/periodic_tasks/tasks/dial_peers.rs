use std::{collections::HashSet, sync::Arc, time::Duration};

use dkg_network::{NetworkManager, PeerId};
use tokio_util::sync::CancellationToken;

use crate::{
    periodic_tasks::DialPeersDeps, periodic_tasks::runner::run_with_shutdown, node_state::PeerRegistry,
};

const DIAL_PEERS_PERIOD: Duration = Duration::from_secs(30);
const CONCURRENT_PEER_DIALS: usize = 20;

pub(crate) struct DialPeersTask {
    network_manager: Arc<NetworkManager>,
    peer_registry: Arc<PeerRegistry>,
}

impl DialPeersTask {
    pub(crate) fn new(deps: DialPeersDeps) -> Self {
        Self {
            network_manager: deps.network_manager,
            peer_registry: deps.peer_registry,
        }
    }

    /// Returns currently connected peers that are in the shard table.
    async fn get_connected_shard_peers(
        &self,
        shard_peers: &HashSet<PeerId>,
    ) -> Result<HashSet<PeerId>, String> {
        let connected = self
            .network_manager
            .connected_peers()
            .await
            .map_err(|e| format!("failed to get connected peers: {}", e))?;

        Ok(connected
            .into_iter()
            .filter(|peer_id| shard_peers.contains(peer_id))
            .collect())
    }

    /// Filters peers to those eligible for discovery (not self, not connected, not in backoff).
    fn filter_peers_for_discovery(
        &self,
        all_peers: Vec<PeerId>,
        own_peer_id: &PeerId,
        connected_peers: &HashSet<PeerId>,
    ) -> Vec<PeerId> {
        all_peers
            .into_iter()
            .filter(|peer_id| {
                *peer_id != *own_peer_id
                    && !connected_peers.contains(peer_id)
                    && self.check_backoff(peer_id)
            })
            .take(CONCURRENT_PEER_DIALS)
            .collect()
    }

    /// Returns true if peer should be attempted (not in backoff).
    fn check_backoff(&self, peer_id: &PeerId) -> bool {
        if self.peer_registry.should_attempt_discovery(peer_id) {
            return true;
        }

        if let Some(backoff) = self.peer_registry.discovery_backoff(peer_id) {
            tracing::trace!(
                %peer_id,
                backoff_secs = backoff.as_secs(),
                "skipping peer discovery due to backoff"
            );
        }
        false
    }

    pub(crate) async fn run(self, shutdown: CancellationToken) {
        run_with_shutdown("dial_peers", shutdown, || self.execute()).await;
    }

    #[tracing::instrument(
        name = "periodic_tasks.dial_peers",
        skip(self),
        fields(
            own_peer_id = tracing::field::Empty,
            total_peers = tracing::field::Empty,
            connected = tracing::field::Empty,
            in_backoff = tracing::field::Empty,
            to_discover = tracing::field::Empty,
        )
    )]
    async fn execute(&self) -> Duration {
        let repeat = || DIAL_PEERS_PERIOD;
        let own_peer_id = *self.network_manager.peer_id();
        tracing::Span::current().record("own_peer_id", tracing::field::display(&own_peer_id));

        // Get all peer IDs from shard table
        let all_peers = self.peer_registry.get_all_shard_peer_ids();

        let all_peers_set: HashSet<PeerId> = all_peers.iter().copied().collect();
        let total_peers = all_peers.len();
        tracing::Span::current().record("total_peers", tracing::field::display(total_peers));

        // Get currently connected peers
        let connected_peers = match self.get_connected_shard_peers(&all_peers_set).await {
            Ok(peers) => peers,
            Err(e) => {
                tracing::error!(error = %e, "Failed to load connected peers");
                return repeat();
            }
        };

        // Filter to peers we need to discover
        let peers_to_find =
            self.filter_peers_for_discovery(all_peers, &own_peer_id, &connected_peers);

        // Count disconnected peers in backoff (total - self - connected - to_discover)
        let in_backoff = total_peers
            .saturating_sub(1) // self
            .saturating_sub(connected_peers.len())
            .saturating_sub(peers_to_find.len());

        let span = tracing::Span::current();
        span.record("connected", tracing::field::display(connected_peers.len()));
        span.record("in_backoff", tracing::field::display(in_backoff));
        span.record("to_discover", tracing::field::display(peers_to_find.len()));

        if peers_to_find.is_empty() {
            if in_backoff > 0 {
                tracing::debug!(
                    total_peers,
                    connected = connected_peers.len(),
                    in_backoff,
                    "no peers to discover, {} disconnected peers in backoff",
                    in_backoff,
                );
            } else {
                tracing::debug!(
                    total_peers,
                    connected = connected_peers.len(),
                    "all shard peers are connected"
                );
            }
            return repeat();
        }

        tracing::debug!(
            to_discover = peers_to_find.len(),
            connected = connected_peers.len(),
            in_backoff,
            "discovering disconnected peers via DHT"
        );

        if let Err(e) = self.network_manager.find_peers(peers_to_find).await {
            tracing::error!(error = %e, "failed to find peers");
        }

        repeat()
    }
}
