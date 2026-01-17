use std::sync::Arc;

use async_trait::async_trait;
use libp2p::PeerId;
use network::NetworkManager;
use repository::RepositoryManager;

use crate::{
    commands::{command::CommandHandler, command_executor::CommandExecutionResult},
    context::Context,
    network::NetworkProtocols,
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

#[async_trait]
impl CommandHandler<DialPeersCommandData> for DialPeersCommandHandler {
    async fn execute(&self, _: &DialPeersCommandData) -> CommandExecutionResult {
        let peer_id = self.network_manager.peer_id().to_base58();

        let potential_peer_ids = self
            .repository_manager
            .shard_repository()
            .get_peers_to_dial(DIAL_CONCURRENCY, MIN_DIAL_FREQUENCY_PER_PEER_MS)
            .await
            .unwrap();

        let peer_ids: Vec<_> = potential_peer_ids
            .into_iter()
            .filter(|p| p != &peer_id)
            .collect();

        if !peer_ids.is_empty() {
            tracing::info!("Dialing {} remote peers", peer_ids.len());

            for peer_id in &peer_ids {
                tracing::trace!("Dialing peer: {}...", peer_id);
            }

            let peers: Vec<PeerId> = peer_ids
                .iter()
                .map(|peer_id| peer_id.parse::<PeerId>().unwrap()) // TODO: Consider handling this unwrap.
                .collect();
            let _ = self.network_manager.dial_peers(peers).await;
        }

        CommandExecutionResult::Repeat {
            delay_ms: DIAL_PEERS_COMMAND_PERIOD_MS,
        }
    }
}
