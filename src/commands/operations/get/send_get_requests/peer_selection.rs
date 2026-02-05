use libp2p::PeerId;
use uuid::Uuid;

use super::handler::SendGetRequestsCommandHandler;
use crate::{
    commands::command_executor::CommandExecutionResult,
    managers::network::protocols::{GetProtocol, ProtocolSpec},
    types::ParsedUal,
};

impl SendGetRequestsCommandHandler {
    pub(crate) async fn load_shard_peers(
        &self,
        operation_id: Uuid,
        parsed_ual: &ParsedUal,
        paranet_ual: Option<&str>,
    ) -> Result<Vec<PeerId>, CommandExecutionResult> {
        // Get shard nodes for the blockchain
        let shard_nodes = match self
            .repository_manager
            .shard_repository()
            .get_all_peer_records(parsed_ual.blockchain.as_str())
            .await
        {
            Ok(nodes) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    shard_nodes_count = nodes.len(),
                    "Loaded shard nodes from repository"
                );
                nodes
            }
            Err(e) => {
                let error_message = format!("Failed to get shard nodes: {}", e);
                tracing::error!(operation_id = %operation_id, error = %e, "Failed to get shard nodes");
                self.get_operation_status_service
                    .mark_failed(operation_id, error_message)
                    .await;
                return Err(CommandExecutionResult::Completed);
            }
        };

        // Filter out self and parse peer IDs
        let my_peer_id = *self.network_manager.peer_id();
        let all_shard_peers: Vec<PeerId> = shard_nodes
            .iter()
            .filter_map(|record| record.peer_id.parse().ok())
            .filter(|peer_id| *peer_id != my_peer_id)
            .collect();

        // Apply paranet filtering if paranet_ual is provided
        let peers: Vec<PeerId> = if let Some(paranet_ual) = paranet_ual {
            tracing::debug!(
                operation_id = %operation_id,
                paranet_ual = %paranet_ual,
                "Applying paranet node filtering"
            );
            match self
                .handle_paranet_node_selection(
                    operation_id,
                    paranet_ual,
                    parsed_ual,
                    all_shard_peers,
                )
                .await
            {
                Ok(filtered_peers) => filtered_peers,
                Err(()) => {
                    // Operation already marked as failed in handle_paranet_node_selection
                    return Err(CommandExecutionResult::Completed);
                }
            }
        } else {
            all_shard_peers
        };

        Ok(self
            .network_manager
            .filter_peers_by_protocol(peers, GetProtocol::STREAM_PROTOCOL))
    }
}
