use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_domain::{Assertion, TokenIds, parse_ual};
use dkg_network::{GetAck, NetworkManager, PeerId, ResponseHandle};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    application::{TripleStoreAssertions, paranet},
    commands::HandleGetRequestDeps,
    commands::{executor::CommandOutcome, registry::CommandHandler},
    node_state::PeerRegistry,
    node_state::ResponseChannels,
};

/// Command data for handling incoming get requests.
#[derive(Clone)]
pub(crate) struct HandleGetRequestCommandData {
    pub operation_id: Uuid,
    pub ual: String,
    pub token_ids: TokenIds,
    pub include_metadata: bool,
    pub paranet_ual: Option<String>,
    pub remote_peer_id: PeerId,
}

impl HandleGetRequestCommandData {
    pub(crate) fn new(
        operation_id: Uuid,
        ual: String,
        token_ids: TokenIds,
        include_metadata: bool,
        paranet_ual: Option<String>,
        remote_peer_id: PeerId,
    ) -> Self {
        Self {
            operation_id,
            ual,
            token_ids,
            include_metadata,
            paranet_ual,
            remote_peer_id,
        }
    }
}

pub(crate) struct HandleGetRequestCommandHandler {
    pub(super) network_manager: Arc<NetworkManager>,
    triple_store_assertions: Arc<TripleStoreAssertions>,
    peer_registry: Arc<PeerRegistry>,
    response_channels: Arc<ResponseChannels<GetAck>>,
    pub(super) blockchain_manager: Arc<BlockchainManager>,
}

impl HandleGetRequestCommandHandler {
    pub(crate) fn new(deps: HandleGetRequestDeps) -> Self {
        Self {
            network_manager: deps.network_manager,
            triple_store_assertions: deps.triple_store_assertions,
            peer_registry: deps.peer_registry,
            response_channels: deps.get_response_channels,
            blockchain_manager: deps.blockchain_manager,
        }
    }

    pub(crate) async fn send_nack(
        &self,
        channel: ResponseHandle<GetAck>,
        operation_id: Uuid,
        error_message: &str,
    ) {
        if let Err(e) = self
            .network_manager
            .send_get_nack(channel, operation_id, error_message)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send get NACK response"
            );
        }
    }

    pub(crate) async fn send_ack(
        &self,
        channel: ResponseHandle<GetAck>,
        operation_id: Uuid,
        assertion: Assertion,
        metadata: Option<Vec<String>>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_get_ack(
                channel,
                operation_id,
                GetAck {
                    assertion,
                    metadata,
                },
            )
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send get ACK response"
            );
        }
    }
}

impl CommandHandler<HandleGetRequestCommandData> for HandleGetRequestCommandHandler {
    #[instrument(
        name = "op.get.recv",
        skip(self, data),
        fields(
            operation_id = %data.operation_id,
            protocol = "get",
            direction = "recv",
            ual = %data.ual,
            remote_peer = %data.remote_peer_id,
            include_metadata = data.include_metadata,
            paranet = ?data.paranet_ual,
            effective_visibility = tracing::field::Empty,
        )
    )]
    async fn execute(&self, data: &HandleGetRequestCommandData) -> CommandOutcome {
        let operation_id = data.operation_id;
        let ual = &data.ual;
        let remote_peer_id = &data.remote_peer_id;

        // Retrieve the response channel
        let Some(channel) = self
            .response_channels
            .retrieve(remote_peer_id, operation_id)
        else {
            tracing::warn!(
                operation_id = %operation_id,
                peer = %remote_peer_id,
                "Response channel not found; request may have expired"
            );
            return CommandOutcome::Completed;
        };

        // Parse the UAL
        let parsed_ual = match parse_ual(ual) {
            Ok(parsed) => parsed,
            Err(e) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    ual = %ual,
                    error = %e,
                    "Failed to parse UAL - sending NACK"
                );
                self.send_nack(channel, operation_id, &format!("Invalid UAL: {}", e))
                    .await;
                return CommandOutcome::Completed;
            }
        };

        // Only serve get requests if this node is part of the shard for the request's blockchain.
        let local_peer_id = self.network_manager.peer_id();
        let blockchain = &parsed_ual.blockchain;
        if !self
            .peer_registry
            .is_peer_in_shard(blockchain, local_peer_id)
        {
            tracing::warn!(
                operation_id = %operation_id,
                local_peer_id = %local_peer_id,
                blockchain = %blockchain,
                "Local node not found in shard - sending NACK"
            );
            self.send_nack(channel, operation_id, "Local node not in shard")
                .await;
            return CommandOutcome::Completed;
        }

        // Determine effective visibility based on paranet authorization
        // For PERMISSIONED paranets where both peers are authorized, returns All visibility
        // Otherwise returns Public visibility
        let effective_visibility = paranet::determine_get_visibility_for_paranet(
            self.blockchain_manager.as_ref(),
            &parsed_ual,
            data.paranet_ual.as_deref(),
            local_peer_id,
            remote_peer_id,
        )
        .await;
        tracing::Span::current().record(
            "effective_visibility",
            tracing::field::debug(&effective_visibility),
        );

        // Query the triple store using the shared service
        let query_result = self
            .triple_store_assertions
            .query_assertion(
                &parsed_ual,
                &data.token_ids,
                effective_visibility,
                data.include_metadata,
            )
            .await;

        match query_result {
            Ok(Some(result)) if result.assertion.has_data() => {
                self.send_ack(channel, operation_id, result.assertion, result.metadata)
                    .await;
            }
            Ok(_) => {
                self.send_nack(
                    channel,
                    operation_id,
                    &format!("Unable to find assertion {}", ual),
                )
                .await;
            }
            Err(e) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    ual = %ual,
                    error = %e,
                    "Triple store query failed; sending NACK"
                );
                self.send_nack(
                    channel,
                    operation_id,
                    &format!("Triple store query failed: {}", e),
                )
                .await;
            }
        }

        CommandOutcome::Completed
    }
}
