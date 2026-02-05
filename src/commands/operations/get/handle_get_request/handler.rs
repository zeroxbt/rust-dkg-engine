use std::sync::Arc;

use libp2p::PeerId;
use tracing::instrument;
use uuid::Uuid;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    managers::{
        blockchain::BlockchainManager,
        network::{NetworkManager, messages::GetAck},
        triple_store::TokenIds,
    },
    services::{ResponseChannels, TripleStoreService},
    types::parse_ual,
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
    triple_store_service: Arc<TripleStoreService>,
    response_channels: Arc<ResponseChannels<GetAck>>,
    pub(super) blockchain_manager: Arc<BlockchainManager>,
}

impl HandleGetRequestCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            network_manager: Arc::clone(context.network_manager()),
            triple_store_service: Arc::clone(context.triple_store_service()),
            response_channels: Arc::clone(context.get_response_channels()),
            blockchain_manager: Arc::clone(context.blockchain_manager()),
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
    async fn execute(&self, data: &HandleGetRequestCommandData) -> CommandExecutionResult {
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
            return CommandExecutionResult::Completed;
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
                return CommandExecutionResult::Completed;
            }
        };

        // Determine effective visibility based on paranet authorization
        // For PERMISSIONED paranets where both peers are authorized, returns All visibility
        // Otherwise returns Public visibility
        let effective_visibility = self
            .determine_visibility_for_paranet(
                &parsed_ual,
                data.paranet_ual.as_deref(),
                remote_peer_id,
            )
            .await;
        tracing::Span::current().record(
            "effective_visibility",
            tracing::field::debug(&effective_visibility),
        );

        tracing::debug!(
            operation_id = %operation_id,
            effective_visibility = ?effective_visibility,
            "Determined effective visibility for query"
        );

        // Query the triple store using the shared service
        let query_result = self
            .triple_store_service
            .query_assertion(
                &parsed_ual,
                &data.token_ids,
                effective_visibility,
                data.include_metadata,
            )
            .await;

        match query_result {
            Ok(Some(result)) if result.assertion.has_data() => {
                tracing::debug!(
                    operation_id = %operation_id,
                    ual = %ual,
                    public_count = result.assertion.public.len(),
                    has_private = result.assertion.private.is_some(),
                    has_metadata = result.metadata.is_some(),
                    "Found assertion data; sending ACK"
                );

                self.send_ack(channel, operation_id, result.assertion, result.metadata)
                    .await;
            }
            Ok(_) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    ual = %ual,
                    "No assertion data found - sending NACK"
                );
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

        CommandExecutionResult::Completed
    }
}
