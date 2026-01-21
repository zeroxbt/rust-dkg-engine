use std::sync::Arc;

use libp2p::PeerId;
use network::{
    NetworkManager, ResponseMessage,
    message::{ResponseMessageHeader, ResponseMessageType},
    request_response::ResponseChannel,
};
use uuid::Uuid;

use triple_store::{Assertion, TokenIds, Visibility};

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    controllers::rpc_controller::{
        NetworkProtocols, ProtocolResponse,
        messages::GetResponseData,
    },
    services::{ResponseChannels, TripleStoreService},
    utils::ual::parse_ual,
};

/// Command data for handling incoming get requests.
#[derive(Clone)]
pub struct HandleGetRequestCommandData {
    pub operation_id: Uuid,
    pub ual: String,
    pub token_ids: TokenIds,
    pub include_metadata: bool,
    pub paranet_ual: Option<String>,
    pub visibility: Visibility,
    pub remote_peer_id: PeerId,
}

impl HandleGetRequestCommandData {
    pub fn new(
        operation_id: Uuid,
        ual: String,
        token_ids: TokenIds,
        include_metadata: bool,
        paranet_ual: Option<String>,
        visibility: Visibility,
        remote_peer_id: PeerId,
    ) -> Self {
        Self {
            operation_id,
            ual,
            token_ids,
            include_metadata,
            paranet_ual,
            visibility,
            remote_peer_id,
        }
    }
}

pub struct HandleGetRequestCommandHandler {
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    triple_store_service: Arc<TripleStoreService>,
    response_channels: Arc<ResponseChannels<GetResponseData>>,
}

impl HandleGetRequestCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            network_manager: Arc::clone(context.network_manager()),
            triple_store_service: Arc::clone(context.triple_store_service()),
            response_channels: Arc::clone(context.get_response_channels()),
        }
    }

    async fn send_response(
        &self,
        channel: ResponseChannel<ResponseMessage<GetResponseData>>,
        operation_id: Uuid,
        message: ResponseMessage<GetResponseData>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_protocol_response(ProtocolResponse::Get { channel, message })
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send get response"
            );
        }
    }

    async fn send_nack(
        &self,
        channel: ResponseChannel<ResponseMessage<GetResponseData>>,
        operation_id: Uuid,
        error_message: &str,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader {
                operation_id,
                message_type: ResponseMessageType::Nack,
            },
            data: GetResponseData::error(error_message),
        };
        self.send_response(channel, operation_id, message).await;
    }

    async fn send_ack(
        &self,
        channel: ResponseChannel<ResponseMessage<GetResponseData>>,
        operation_id: Uuid,
        assertion: Assertion,
        metadata: Option<Vec<String>>,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader {
                operation_id,
                message_type: ResponseMessageType::Ack,
            },
            data: GetResponseData::data(assertion, metadata),
        };
        self.send_response(channel, operation_id, message).await;
    }
}

// TODO: add paranet support
impl CommandHandler<HandleGetRequestCommandData> for HandleGetRequestCommandHandler {
    async fn execute(&self, data: &HandleGetRequestCommandData) -> CommandExecutionResult {
        let operation_id = data.operation_id;
        let ual = &data.ual;
        let remote_peer_id = &data.remote_peer_id;
        let visibility = data.visibility;

        tracing::info!(
            operation_id = %operation_id,
            ual = %ual,
            remote_peer_id = %remote_peer_id,
            visibility = ?visibility,
            include_metadata = data.include_metadata,
            "Starting HandleGetRequest command"
        );

        // Retrieve the response channel
        let Some(channel) = self
            .response_channels
            .retrieve(remote_peer_id, operation_id)
        else {
            tracing::error!(
                operation_id = %operation_id,
                peer = %remote_peer_id,
                "No cached response channel found. Channel may have expired."
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

        // TODO: Handle paranet access policy validation
        // For permissioned paranets, we need to:
        // 1. Check if the knowledge collection is registered in the paranet
        // 2. Check if both remote and local peer are in the paranet
        // 3. Return ALL visibility triples (public + private) for permissioned
        // For now, we skip paranet validation and use Visibility::Public

        // Query the triple store using the shared service
        let query_result = self
            .triple_store_service
            .query_assertion(
                &parsed_ual,
                &data.token_ids,
                Visibility::Public,
                data.include_metadata,
            )
            .await;

        match query_result {
            Some(result) if result.has_data() => {
                tracing::info!(
                    operation_id = %operation_id,
                    ual = %ual,
                    public_count = result.public.len(),
                    has_private = result.private.is_some(),
                    has_metadata = result.metadata.is_some(),
                    "Found assertion data - sending ACK"
                );

                let assertion = Assertion {
                    public: result.public,
                    private: result.private,
                };

                self.send_ack(channel, operation_id, assertion, result.metadata)
                    .await;
            }
            _ => {
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
        }

        CommandExecutionResult::Completed
    }
}
