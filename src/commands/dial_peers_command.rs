use super::command::CommandName;
use crate::commands::command::{AbstractCommand, CommandExecutionResult, CoreCommand};
use crate::context::Context;
use async_trait::async_trait;
use repository::models::command;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

const DIAL_PEERS_FREQUENCY_MILLS: i64 = 30_000;
const DIAL_PEERS_CONCURRENCY: usize = 5;
const MIN_DIAL_FREQUENCY_MILLIS: i64 = 60 * 60 * 1000;

#[derive(Clone)]
pub struct DialPeersCommand {
    core: CoreCommand,
    data: DialPeersCommandData,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DialPeersCommandData;

#[async_trait]
impl AbstractCommand for DialPeersCommand {
    async fn execute(&self, context: &Arc<Context>) -> CommandExecutionResult {
        let peer_id = context.network_manager().get_peer_id().to_base58();

        let potential_peer_ids = context
            .repository_manager()
            .shard_repository()
            .get_peers_to_dial(DIAL_PEERS_CONCURRENCY, MIN_DIAL_FREQUENCY_MILLIS)
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

                context
                    .network_action_tx()
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

    fn core(&self) -> &CoreCommand {
        &self.core
    }

    fn json_data(&self) -> Value {
        serde_json::to_value(&self.data).unwrap()
    }
}

impl Default for DialPeersCommand {
    fn default() -> Self {
        Self {
            core: CoreCommand {
                name: CommandName::DialPeers,
                period: Some(DIAL_PEERS_FREQUENCY_MILLS),
                ..CoreCommand::default()
            },
            data: DialPeersCommandData {},
        }
    }
}

impl From<command::Model> for DialPeersCommand {
    fn from(model: command::Model) -> Self {
        Self {
            core: CoreCommand::from_model(model.clone()),
            data: serde_json::from_value(model.data).unwrap(),
        }
    }
}
