use std::{collections::HashSet, sync::Arc, time::Duration};

use libp2p::PeerId;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    controllers::rpc_controller::NetworkProtocols,
    managers::{network::NetworkManager, repository::RepositoryManager},
    services::PeerDiscoveryTracker,
};

/// Interval between peer discovery attempts (30 seconds)
const DIAL_PEERS_PERIOD: Duration = Duration::from_secs(30);
const DIAL_BATCH_SIZE: usize = 10;

pub(crate) struct DialPeersCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
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
}

#[derive(Clone, Default)]
pub(crate) struct DialPeersCommandData;

impl CommandHandler<DialPeersCommandData> for DialPeersCommandHandler {
    async fn execute(&self, _: &DialPeersCommandData) -> CommandExecutionResult {
        let own_peer_id = *self.network_manager.peer_id();

        // Get all peer IDs from shard table
        let all_peer_ids = match self
            .repository_manager
            .shard_repository()
            .get_all_peer_ids()
            .await
        {
            Ok(peers) => peers,
            Err(e) => {
                tracing::error!(error = %e, "failed to fetch peer ids from shard table");
                return CommandExecutionResult::Repeat {
                    delay: DIAL_PEERS_PERIOD,
                };
            }
        };

        // Get currently connected peers
        let connected_peers: HashSet<PeerId> = match self.network_manager.connected_peers().await {
            Ok(peers) => peers.into_iter().collect(),
            Err(e) => {
                tracing::error!(error = %e, "failed to get connected peers");
                return CommandExecutionResult::Repeat {
                    delay: DIAL_PEERS_PERIOD,
                };
            }
        };

        let total_peers = all_peer_ids.len();

        // Filter to peers we're not connected to and not in backoff
        let peers_to_find: Vec<PeerId> = all_peer_ids
            .into_iter()
            .filter_map(|peer_id_str| match peer_id_str.parse::<PeerId>() {
                Ok(peer_id) => Some(peer_id),
                Err(e) => {
                    tracing::warn!(%peer_id_str, error = %e, "invalid peer id in shard table");
                    None
                }
            })
            .filter(|peer_id| {
                if *peer_id == own_peer_id || connected_peers.contains(peer_id) {
                    return false;
                }
                // Check backoff - skip peers we recently failed to find
                if !self.peer_discovery_tracker.should_attempt(peer_id) {
                    if let Some(backoff) = self.peer_discovery_tracker.get_backoff(peer_id) {
                        tracing::trace!(
                            %peer_id,
                            backoff_secs = backoff.as_secs(),
                            "skipping peer discovery due to backoff"
                        );
                    }
                    return false;
                }
                true
            })
            .take(DIAL_BATCH_SIZE)
            .collect();

        if peers_to_find.is_empty() {
            tracing::debug!(
                total_peers,
                connected = connected_peers.len(),
                "all shard table peers are connected"
            );
            return CommandExecutionResult::Repeat {
                delay: DIAL_PEERS_PERIOD,
            };
        }

        tracing::info!(
            count = peers_to_find.len(),
            connected = connected_peers.len(),
            "discovering disconnected peers via DHT"
        );

        if let Err(e) = self.network_manager.find_peers(peers_to_find).await {
            tracing::error!(error = %e, "failed to find peers");
        }

        CommandExecutionResult::Repeat {
            delay: DIAL_PEERS_PERIOD,
        }
    }
}
