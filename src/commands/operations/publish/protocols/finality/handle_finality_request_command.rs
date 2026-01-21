use std::sync::Arc;

use libp2p::PeerId;
use network::{
    NetworkManager, ResponseMessage,
    message::{ResponseMessageHeader, ResponseMessageType},
    request_response::ResponseChannel,
};
use repository::RepositoryManager;
use uuid::Uuid;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    controllers::rpc_controller::{
        NetworkProtocols, ProtocolResponse, messages::FinalityResponseData,
    },
    services::ResponseChannels,
};

/// Command data for handling incoming finality requests from storage nodes.
/// This runs on the publisher node when a storage node confirms it has stored the data.
#[derive(Clone)]
pub struct HandleFinalityRequestCommandData {
    /// The operation ID for this finality request
    pub operation_id: Uuid,
    /// The UAL (Universal Asset Locator) of the knowledge collection
    pub ual: String,
    /// The original publish operation ID
    pub publish_operation_id: String,
    /// The peer ID of the storage node sending the finality request
    pub remote_peer_id: PeerId,
}

impl HandleFinalityRequestCommandData {
    pub fn new(
        operation_id: Uuid,
        ual: String,
        publish_operation_id: String,
        remote_peer_id: PeerId,
    ) -> Self {
        Self {
            operation_id,
            ual,
            publish_operation_id,
            remote_peer_id,
        }
    }
}

pub struct HandleFinalityRequestCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    response_channels: Arc<ResponseChannels<FinalityResponseData>>,
}

impl HandleFinalityRequestCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            response_channels: Arc::clone(context.finality_response_channels()),
        }
    }

    async fn send_response(
        &self,
        channel: ResponseChannel<ResponseMessage<FinalityResponseData>>,
        operation_id: Uuid,
        message: ResponseMessage<FinalityResponseData>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_protocol_response(ProtocolResponse::Finality { channel, message })
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send finality response"
            );
        }
    }

    async fn send_ack(
        &self,
        channel: ResponseChannel<ResponseMessage<FinalityResponseData>>,
        operation_id: Uuid,
        ual: &str,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader {
                operation_id,
                message_type: ResponseMessageType::Ack,
            },
            data: FinalityResponseData::Ack {
                message: format!("Acknowledged storing of {}", ual),
            },
        };
        self.send_response(channel, operation_id, message).await;
    }

    async fn send_nack(
        &self,
        channel: ResponseChannel<ResponseMessage<FinalityResponseData>>,
        operation_id: Uuid,
        ual: &str,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader {
                operation_id,
                message_type: ResponseMessageType::Nack,
            },
            data: FinalityResponseData::Nack {
                error_message: format!("Failed to acknowledge storing of {}", ual),
            },
        };
        self.send_response(channel, operation_id, message).await;
    }
}

impl CommandHandler<HandleFinalityRequestCommandData> for HandleFinalityRequestCommandHandler {
    async fn execute(&self, data: &HandleFinalityRequestCommandData) -> CommandExecutionResult {
        let operation_id = data.operation_id;
        let ual = &data.ual;
        let publish_operation_id = &data.publish_operation_id;
        let remote_peer_id = &data.remote_peer_id;

        tracing::info!(
            operation_id = %operation_id,
            publish_operation_id = %publish_operation_id,
            ual = %ual,
            remote_peer_id = %remote_peer_id,
            "Handling finality request from storage node"
        );

        // Retrieve the response channel
        let Some(channel) = self
            .response_channels
            .retrieve(remote_peer_id, operation_id)
        else {
            tracing::error!(
                operation_id = %operation_id,
                peer = %remote_peer_id,
                "No cached response channel found for finality request. Channel may have expired."
            );
            return CommandExecutionResult::Completed;
        };

        // Parse the publish operation ID
        let publish_op_uuid = match Uuid::parse_str(publish_operation_id) {
            Ok(uuid) => uuid,
            Err(e) => {
                tracing::error!(
                    operation_id = %operation_id,
                    publish_operation_id = %publish_operation_id,
                    error = %e,
                    "Failed to parse publish_operation_id as UUID"
                );
                self.send_nack(channel, operation_id, ual).await;
                return CommandExecutionResult::Completed;
            }
        };

        // Save the finality ack to the database
        if let Err(e) = self
            .repository_manager
            .finality_status_repository()
            .save_finality_ack(publish_op_uuid, ual, &remote_peer_id.to_base58())
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                publish_operation_id = %publish_operation_id,
                ual = %ual,
                peer = %remote_peer_id,
                error = %e,
                "Failed to save finality ack"
            );
            self.send_nack(channel, operation_id, ual).await;
            return CommandExecutionResult::Completed;
        }

        tracing::info!(
            operation_id = %operation_id,
            publish_operation_id = %publish_operation_id,
            ual = %ual,
            peer = %remote_peer_id,
            "Finality ack saved successfully"
        );

        // Send ACK response back to the storage node
        tracing::debug!(
            operation_id = %operation_id,
            peer = %remote_peer_id,
            "Sending finality ACK response"
        );

        self.send_ack(channel, operation_id, ual).await;

        tracing::info!(
            operation_id = %operation_id,
            peer = %remote_peer_id,
            ual = %ual,
            "Finality request handled successfully"
        );

        CommandExecutionResult::Completed
    }
}
