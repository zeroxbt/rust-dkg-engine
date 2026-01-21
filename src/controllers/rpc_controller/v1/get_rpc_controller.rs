use std::sync::Arc;

use network::{
    PeerId,
    message::{RequestMessage, ResponseMessage, ResponseMessageType},
    request_response,
};
use tokio::sync::mpsc::Sender;

use crate::{
    commands::{
        command_executor::CommandExecutionRequest,
        command_registry::Command,
        operations::get::protocols::get::handle_get_request_command::HandleGetRequestCommandData,
    },
    context::Context,
    controllers::rpc_controller::messages::{GetRequestData, GetResponseData},
    services::{GetOperationContextStore, GetValidationService, OperationService, ResponseChannels},
    utils::ual::ParsedUal,
};

pub struct GetRpcController {
    get_operation_manager: Arc<OperationService>,
    get_validation_service: Arc<GetValidationService>,
    get_operation_context_store: Arc<GetOperationContextStore>,
    response_channels: Arc<ResponseChannels<GetResponseData>>,
    schedule_command_tx: Sender<CommandExecutionRequest>,
}

impl GetRpcController {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            get_operation_manager: Arc::clone(context.get_operation_manager()),
            get_validation_service: Arc::clone(context.get_validation_service()),
            get_operation_context_store: Arc::clone(context.get_operation_context_store()),
            response_channels: Arc::clone(context.get_response_channels()),
            schedule_command_tx: context.schedule_command_tx().clone(),
        }
    }

    pub async fn handle_request(
        &self,
        request: RequestMessage<GetRequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<GetResponseData>>,
        remote_peer_id: PeerId,
    ) {
        let RequestMessage { header, data } = request;

        let operation_id = header.operation_id;

        tracing::trace!(
            operation_id = %operation_id,
            ual = %data.ual,
            peer = %remote_peer_id,
            "Get request received"
        );

        // Store channel for later retrieval by command handler
        self.response_channels
            .store(&remote_peer_id, operation_id, channel);

        // Schedule command to handle the get request
        let command = Command::HandleGetRequest(HandleGetRequestCommandData::new(
            operation_id,
            data.ual,
            data.token_ids,
            data.include_metadata,
            data.paranet_ual,
            data.content_type,
            remote_peer_id,
        ));

        let request = CommandExecutionRequest::new(command);

        let command_name = request.command.name();
        if let Err(e) = self.schedule_command_tx.send(request).await {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                command_name = %command_name,
                "Failed to schedule HandleGetRequest command."
            );
        }
    }

    pub async fn handle_response(&self, response: ResponseMessage<GetResponseData>, peer: PeerId) {
        let ResponseMessage { header, data } = response;

        let operation_id = header.operation_id;
        let is_ack = header.message_type == ResponseMessageType::Ack;

        match &data {
            GetResponseData::Data { assertion, metadata } => {
                tracing::debug!(
                    operation_id = %operation_id,
                    peer = %peer,
                    public_triples = assertion.public.len(),
                    has_private = assertion.private.is_some(),
                    has_metadata = metadata.is_some(),
                    "Get response received with data"
                );
            }
            GetResponseData::Error { error_message } => {
                tracing::debug!(
                    operation_id = %operation_id,
                    peer = %peer,
                    error = %error_message,
                    "Get response received with error"
                );
            }
        }

        // Determine success: ACK + valid data (if present)
        let is_success = if is_ack {
            match &data {
                GetResponseData::Data { assertion, .. } => {
                    // Retrieve operation context for validation
                    match self.get_operation_context_store.get(&operation_id) {
                        Some(ctx) => {
                            // Build a ParsedUal from the stored context for validation
                            // Note: We don't have the contract address stored, but validation
                            // only needs blockchain, knowledge_collection_id, and knowledge_asset_id
                            let parsed_ual = ParsedUal {
                                blockchain: ctx.blockchain.clone(),
                                // Use a dummy contract address - validation doesn't use it
                                contract: blockchain::Address::ZERO,
                                knowledge_collection_id: ctx.knowledge_collection_id,
                                knowledge_asset_id: ctx.knowledge_asset_id,
                            };

                            let is_valid = self
                                .get_validation_service
                                .validate_response(
                                    &assertion.public,
                                    assertion.private.as_deref(),
                                    &parsed_ual,
                                    ctx.visibility,
                                )
                                .await;

                            if !is_valid {
                                tracing::debug!(
                                    operation_id = %operation_id,
                                    peer = %peer,
                                    "Network response validation failed"
                                );
                            }

                            is_valid
                        }
                        None => {
                            // No context found - this could happen if:
                            // 1. The operation completed locally (context never stored)
                            // 2. TTL expired (very unlikely within operation timeout)
                            // 3. Context was already removed
                            // Accept the response but log a warning
                            tracing::warn!(
                                operation_id = %operation_id,
                                "No operation context found for validation, accepting response"
                            );
                            true
                        }
                    }
                }
                GetResponseData::Error { .. } => false,
            }
        } else {
            false
        };

        // Record response using operation manager
        if let Err(e) = self
            .get_operation_manager
            .record_response(operation_id, is_success)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to record get response"
            );
        } else {
            tracing::debug!(
                operation_id = %operation_id,
                success = is_success,
                "Get response processed"
            );
        }

        // Clean up context if operation is complete (success or all responses received)
        // Note: This is a simple cleanup; the TTL will handle any stragglers
        if is_success {
            self.get_operation_context_store.remove(&operation_id);
        }
    }
}
