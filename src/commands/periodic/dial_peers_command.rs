use std::sync::Arc;

use libp2p::PeerId;
use network::NetworkManager;
use repository::RepositoryManager;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    controllers::rpc_controller::NetworkProtocols,
};

const DIAL_PEERS_COMMAND_PERIOD_MS: i64 = 10_000;
const DIAL_CONCURRENCY: usize = 5;
const MIN_DIAL_FREQUENCY_PER_PEER_MS: i64 = 60 * 60 * 1000;

pub struct DialPeersCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
}

impl DialPeersCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
        }
    }
}

#[derive(Clone, Default)]
pub struct DialPeersCommandData;

impl CommandHandler<DialPeersCommandData> for DialPeersCommandHandler {
    async fn execute(&self, _: &DialPeersCommandData) -> CommandExecutionResult {
        let own_peer_id = self.network_manager.peer_id().to_base58();

        let potential_peers = match self
            .repository_manager
            .shard_repository()
            .get_peers_to_dial(DIAL_CONCURRENCY, MIN_DIAL_FREQUENCY_PER_PEER_MS)
            .await
        {
            Ok(peers) => peers,
            Err(e) => {
                tracing::error!(error = %e, "failed to fetch peers to dial");
                return CommandExecutionResult::Repeat {
                    delay_ms: DIAL_PEERS_COMMAND_PERIOD_MS,
                };
            }
        };

        let peers: Vec<PeerId> = potential_peers
            .into_iter()
            .filter(|p| p != &own_peer_id)
            .filter_map(|peer_id| match peer_id.parse::<PeerId>() {
                Ok(id) => {
                    tracing::trace!(%peer_id, "dialing peer");
                    Some(id)
                }
                Err(e) => {
                    tracing::warn!(%peer_id, error = %e, "invalid peer id");
                    None
                }
            })
            .collect();

        if peers.is_empty() {
            tracing::debug!("no peers to dial");
            return CommandExecutionResult::Repeat {
                delay_ms: DIAL_PEERS_COMMAND_PERIOD_MS,
            };
        }

        tracing::info!(count = peers.len(), "dialing remote peers");

        if let Err(e) = self.network_manager.dial_peers(peers).await {
            tracing::error!(error = %e, "failed to dial peers");
        }

        CommandExecutionResult::Repeat {
            delay_ms: DIAL_PEERS_COMMAND_PERIOD_MS,
        }
    }
}
