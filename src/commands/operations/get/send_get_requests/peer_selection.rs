use libp2p::PeerId;
use uuid::Uuid;

use super::handler::SendGetRequestsCommandHandler;
use crate::{
    commands::executor::CommandOutcome,
    managers::network::protocols::{GetProtocol, ProtocolSpec},
    dkg_domain::ParsedUal,
};

impl SendGetRequestsCommandHandler {
    pub(crate) async fn load_shard_peers(
        &self,
        operation_id: Uuid,
        parsed_ual: &ParsedUal,
        paranet_ual: Option<&str>,
    ) -> Result<Vec<PeerId>, CommandOutcome> {
        // Get shard peers that support the GET protocol, excluding self
        let my_peer_id = self.network_manager.peer_id();
        let all_shard_peers = self.peer_service.select_shard_peers(
            &parsed_ual.blockchain,
            GetProtocol::STREAM_PROTOCOL,
            Some(my_peer_id),
        );

        tracing::debug!(
            operation_id = %operation_id,
            shard_nodes_count = all_shard_peers.len(),
            "Loaded shard peers from peer service"
        );

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
                    return Err(CommandOutcome::Completed);
                }
            }
        } else {
            all_shard_peers
        };

        Ok(peers)
    }
}
