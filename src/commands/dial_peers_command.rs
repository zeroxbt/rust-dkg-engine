use async_trait::async_trait;
use std::sync::Arc;

use super::command::Command;
use crate::context::Context;
use crate::types::traits::command::{CommandExecutionResult, ScheduleConfig};
use crate::{
    commands::constants::DEFAULT_COMMAND_DELAY_MS, types::traits::command::CommandHandler,
};
use network::action::NetworkAction;
use network::NetworkManager;
use repository::RepositoryManager;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

const DIAL_PEERS_COMMAND_PERIOD_MS: i64 = 30_000;
const DIAL_CONCURRENCY: usize = 5;
const MIN_DIAL_FREQUENCY_PER_PEER_MS: i64 = 60 * 60 * 1000;

#[derive(Serialize, Deserialize, Clone)]
pub struct DialPeersCommandData;

pub struct DialPeersCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager>,
    network_action_tx: mpsc::Sender<NetworkAction>,
}

impl DialPeersCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            network_action_tx: context.network_action_tx().clone(),
        }
    }
}

#[async_trait]
impl CommandHandler for DialPeersCommandHandler {
    fn name(&self) -> &'static str {
        "dialPeersCommand"
    }

    fn schedule_config(&self) -> ScheduleConfig {
        ScheduleConfig::periodic_with_delay(DIAL_PEERS_COMMAND_PERIOD_MS, DEFAULT_COMMAND_DELAY_MS)
    }

    async fn execute(&self, _: &Command) -> CommandExecutionResult {
        let peer_id = self.network_manager.peer_id().to_base58();

        let potential_peer_ids = self
            .repository_manager
            .shard_repository()
            .get_peers_to_dial(DIAL_CONCURRENCY, MIN_DIAL_FREQUENCY_PER_PEER_MS)
            .await
            .unwrap();

        let peer_ids: Vec<_> = potential_peer_ids
            .iter()
            .filter(|p| **p != peer_id)
            .collect();

        if !peer_ids.is_empty() {
            tracing::info!("Dialing {} remote peers", peer_ids.len());

            for peer_id in peer_ids {
                tracing::trace!("Dialing peer: {}...", peer_id);

                self.network_action_tx
                    .send(network::action::NetworkAction::GetClosestPeers {
                        peer: peer_id.parse().unwrap(), // Note: Consider handling this unwrap.
                    })
                    .await
                    .map_err(|e| {
                        tracing::error!("Failed to send network action: {:?}", e);
                        e
                    })
                    .unwrap();
            }
        }

        CommandExecutionResult::Repeat
    }

    async fn recover(&self) -> CommandExecutionResult {
        tracing::warn!("Failed to dial peers: error: ");

        CommandExecutionResult::Repeat
    }
}
