use std::{collections::HashSet, sync::Arc, time::Duration};

use libp2p::PeerId;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    managers::{network::NetworkManager, repository::RepositoryManager},
    services::PeerDiscoveryTracker,
};

const DIAL_PEERS_PERIOD: Duration = Duration::from_secs(30);
const DIAL_BATCH_SIZE: usize = 20;

pub(crate) struct DialPeersCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager>,
    peer_discovery_tracker: Arc<PeerDiscoveryTracker>,
}

impl DialPeersCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            peer_discovery_tracker: Arc::clone(context.peer_discovery_tracker()),
        }
    }

    /// Fetches peer IDs from the shard table and parses them.
    async fn get_shard_peer_ids(&self) -> Result<Vec<PeerId>, String> {
        let peer_id_strings = self
            .repository_manager
            .shard_repository()
            .get_all_peer_ids()
            .await
            .map_err(|e| format!("failed to fetch peer ids from shard table: {}", e))?;

        Ok(peer_id_strings
            .into_iter()
            .filter_map(|s| match s.parse::<PeerId>() {
                Ok(peer_id) => Some(peer_id),
                Err(e) => {
                    tracing::warn!(%s, error = %e, "invalid peer id in shard table");
                    None
                }
            })
            .collect())
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
            .take(DIAL_BATCH_SIZE)
            .collect()
    }

    /// Returns true if peer should be attempted (not in backoff).
    fn check_backoff(&self, peer_id: &PeerId) -> bool {
        if self.peer_discovery_tracker.should_attempt(peer_id) {
            return true;
        }

        if let Some(backoff) = self.peer_discovery_tracker.get_backoff(peer_id) {
            tracing::trace!(
                %peer_id,
                backoff_secs = backoff.as_secs(),
                "skipping peer discovery due to backoff"
            );
        }
        false
    }
}

#[derive(Clone, Default)]
pub(crate) struct DialPeersCommandData;

impl CommandHandler<DialPeersCommandData> for DialPeersCommandHandler {
    async fn execute(&self, _: &DialPeersCommandData) -> CommandExecutionResult {
        let repeat = || CommandExecutionResult::Repeat {
            delay: DIAL_PEERS_PERIOD,
        };
        let own_peer_id = *self.network_manager.peer_id();

        // Get all peer IDs from shard table
        let all_peers = match self.get_shard_peer_ids().await {
            Ok(peers) => peers,
            Err(e) => {
                tracing::error!(error = %e);
                return repeat();
            }
        };

        let all_peers_set: HashSet<PeerId> = all_peers.iter().copied().collect();
        let total_peers = all_peers.len();

        // Get currently connected peers
        let connected_peers = match self.get_connected_shard_peers(&all_peers_set).await {
            Ok(peers) => peers,
            Err(e) => {
                tracing::error!(error = %e);
                return repeat();
            }
        };

        // Filter to peers we need to discover
        let peers_to_find =
            self.filter_peers_for_discovery(all_peers, &own_peer_id, &connected_peers);

        if peers_to_find.is_empty() {
            tracing::debug!(
                total_peers,
                connected = connected_peers.len(),
                "all shard table peers are connected"
            );
            return repeat();
        }

        tracing::info!(
            count = peers_to_find.len(),
            connected = connected_peers.len(),
            "discovering disconnected peers via DHT"
        );

        if let Err(e) = self.network_manager.find_peers(peers_to_find).await {
            tracing::error!(error = %e, "failed to find peers");
        }

        repeat()
    }
}
