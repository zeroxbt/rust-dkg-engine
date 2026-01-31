use std::ops::ControlFlow;

use libp2p::PeerId;
use uuid::Uuid;

use crate::{
    commands::command_executor::CommandExecutionResult,
    managers::network::message::ResponseBody,
    managers::network::messages::FinalityRequestData,
    operations::protocols::publish_finality,
    utils::peer_fanout::{for_each_peer_concurrently, limit_peers},
};

use super::handler::SendPublishFinalityRequestCommandHandler;

impl SendPublishFinalityRequestCommandHandler {
    pub(crate) async fn send_finality_request_to_publisher(
        &self,
        operation_id: Uuid,
        publish_operation_id: Uuid,
        publish_operation_id_str: &str,
        ual: String,
        blockchain: crate::managers::blockchain::BlockchainId,
        publisher_peer_id: PeerId,
    ) -> CommandExecutionResult {
        let finality_request_data = FinalityRequestData::new(
            ual,
            publish_operation_id_str.to_string(),
            blockchain,
        );

        let mut peers = vec![publisher_peer_id];
        peers = limit_peers(peers, publish_finality::MAX_PEERS);

        let min_required_peers =
            publish_finality::MIN_PEERS.max(publish_finality::MIN_ACK_RESPONSES as usize);

        if peers.len() < min_required_peers {
            tracing::warn!(
                operation_id = %operation_id,
                publish_operation_id = %publish_operation_id,
                found = peers.len(),
                required = min_required_peers,
                "Not enough peers to send finality request"
            );
            return CommandExecutionResult::Completed;
        }

        let mut ack_count: u16 = 0;
        for_each_peer_concurrently(
            &peers,
            publish_finality::CONCURRENT_PEERS,
            |peer| {
                let request_data = finality_request_data.clone();
                let network_manager = std::sync::Arc::clone(&self.network_manager);
                async move {
                    let addresses = network_manager
                        .get_peer_addresses(peer)
                        .await
                        .unwrap_or_default();
                    let result = network_manager
                        .send_finality_request(peer, addresses, operation_id, request_data)
                        .await;
                    (peer, result)
                }
            },
            |(peer, result)| {
                match result {
                    Ok(ResponseBody::Ack(_)) => {
                        ack_count = ack_count.saturating_add(1);
                        tracing::info!(
                            operation_id = %operation_id,
                            publish_operation_id = %publish_operation_id,
                            peer = %peer,
                            "Sent finality request to publisher"
                        );
                    }
                    Ok(ResponseBody::Error(_)) => {
                        tracing::warn!(
                            operation_id = %operation_id,
                            publish_operation_id = %publish_operation_id,
                            peer = %peer,
                            "Publisher returned error for finality request"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            operation_id = %operation_id,
                            publish_operation_id = %publish_operation_id,
                            peer = %peer,
                            error = %e,
                            "Failed to send finality request to publisher"
                        );
                    }
                }

                if ack_count >= publish_finality::MIN_ACK_RESPONSES {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            },
        )
        .await;

        CommandExecutionResult::Completed
    }
}
