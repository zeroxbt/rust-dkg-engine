use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_domain::{Assertion, parse_ual};
use dkg_network::{GetAck, GetRequestData, InboundRequest, NetworkManager, ResponseHandle};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    application::{TripleStoreAssertions, paranet},
    commands::{
        HandleGetRequestDeps,
        executor::CommandOutcome,
        registry::{CommandHandler, InboundCommandData},
    },
    peer_registry::PeerRegistry,
};

/// Command data for handling incoming get requests.
pub(crate) struct HandleGetRequestCommandData {
    pub request: InboundRequest<GetRequestData>,
    pub response_handle: ResponseHandle<GetAck>,
}

impl HandleGetRequestCommandData {
    pub(crate) fn new(
        request: InboundRequest<GetRequestData>,
        response_handle: ResponseHandle<GetAck>,
    ) -> Self {
        Self {
            request,
            response_handle,
        }
    }
}

impl InboundCommandData<GetAck> for HandleGetRequestCommandData {
    fn into_response_handle(self) -> ResponseHandle<GetAck> {
        self.response_handle
    }
}

pub(crate) struct HandleGetRequestCommandHandler {
    pub(super) network_manager: Arc<NetworkManager>,
    triple_store_assertions: Arc<TripleStoreAssertions>,
    peer_registry: Arc<PeerRegistry>,
    pub(super) blockchain_manager: Arc<BlockchainManager>,
}

impl HandleGetRequestCommandHandler {
    pub(crate) fn new(deps: HandleGetRequestDeps) -> Self {
        Self {
            network_manager: deps.network_manager,
            triple_store_assertions: deps.triple_store_assertions,
            peer_registry: deps.peer_registry,
            blockchain_manager: deps.blockchain_manager,
        }
    }

    pub(crate) async fn send_nack(
        &self,
        response_handle: ResponseHandle<GetAck>,
        operation_id: Uuid,
        error_message: &str,
    ) {
        if let Err(e) = self
            .network_manager
            .send_get_nack(response_handle, operation_id, error_message)
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
        response_handle: ResponseHandle<GetAck>,
        operation_id: Uuid,
        assertion: Assertion,
        metadata: Option<Vec<String>>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_get_ack(
                response_handle,
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

impl CommandHandler for HandleGetRequestCommandHandler {
    type Data = HandleGetRequestCommandData;

    #[instrument(
        name = "op.get.recv",
        skip(self, data),
        fields(
            operation_id = %data.request.operation_id(),
            protocol = "get",
            direction = "recv",
            ual = tracing::field::Empty,
            remote_peer = %data.request.peer_id(),
            include_metadata = data.request.data().include_metadata(),
            paranet = tracing::field::Empty,
            effective_visibility = tracing::field::Empty,
        )
    )]
    async fn execute(&self, data: Self::Data) -> CommandOutcome {
        let HandleGetRequestCommandData {
            request,
            response_handle,
        } = data;
        let (operation_id, remote_peer_id, request) = request.into_parts();
        let ual = request.ual().to_string();
        let token_ids = request.token_ids_shared();
        let include_metadata = request.include_metadata();
        let paranet_ual = request.paranet_ual_shared();
        let remote_peer_id = &remote_peer_id;
        tracing::Span::current().record("ual", tracing::field::display(&ual));
        tracing::Span::current().record(
            "paranet",
            tracing::field::debug(&paranet_ual.as_deref().map(String::as_str)),
        );

        // Parse the UAL
        let parsed_ual = match parse_ual(&ual) {
            Ok(parsed) => parsed,
            Err(e) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    ual = %ual,
                    error = %e,
                    "Failed to parse UAL - sending NACK"
                );
                self.send_nack(
                    response_handle,
                    operation_id,
                    &format!("Invalid UAL: {}", e),
                )
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
            self.send_nack(response_handle, operation_id, "Local node not in shard")
                .await;
            return CommandOutcome::Completed;
        }

        // Determine effective visibility based on paranet authorization
        // For PERMISSIONED paranets where both peers are authorized, returns All visibility
        // Otherwise returns Public visibility
        let effective_visibility = paranet::determine_get_visibility_for_paranet(
            self.blockchain_manager.as_ref(),
            &parsed_ual,
            paranet_ual.as_deref().map(String::as_str),
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
                token_ids.as_ref(),
                effective_visibility,
                include_metadata,
            )
            .await;

        match query_result {
            Ok(Some(result)) if result.assertion.has_data() => {
                self.send_ack(
                    response_handle,
                    operation_id,
                    result.assertion,
                    result.metadata,
                )
                .await;
            }
            Ok(_) => {
                self.send_nack(
                    response_handle,
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
                    response_handle,
                    operation_id,
                    &format!("Triple store query failed: {}", e),
                )
                .await;
            }
        }

        CommandOutcome::Completed
    }
}
