use std::sync::Arc;

use network::{
    PeerId,
    message::{RequestMessage, ResponseMessage, ResponseMessageType},
    request_response,
};
use triple_store::Assertion;

use crate::{
    commands::{
        command_executor::{CommandExecutionRequest, CommandScheduler},
        command_registry::Command,
        operations::get::protocols::get::handle_get_request_command::HandleGetRequestCommandData,
    },
    context::Context,
    controllers::rpc_controller::messages::{GetRequestData, GetResponseData},
    operations::{GetOperation, GetOperationResult},
    services::{
        GetValidationService, ResponseChannels,
        operation::OperationService as GenericOperationService,
    },
    utils::ual::ParsedUal,
};

pub struct GetRpcController {
    get_operation_service: Arc<GenericOperationService<GetOperation>>,
    get_validation_service: Arc<GetValidationService>,
    response_channels: Arc<ResponseChannels<GetResponseData>>,
    command_scheduler: CommandScheduler,
}

impl GetRpcController {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            get_operation_service: Arc::clone(context.get_operation_service()),
            get_validation_service: Arc::clone(context.get_validation_service()),
            response_channels: Arc::clone(context.get_response_channels()),
            command_scheduler: context.command_scheduler().clone(),
        }
    }

    /// Returns a reference to the operation service for this controller.
    pub fn operation_service(&self) -> &Arc<GenericOperationService<GetOperation>> {
        &self.get_operation_service
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

        self.command_scheduler
            .schedule(CommandExecutionRequest::new(command))
            .await;
    }

    pub async fn handle_response(&self, response: ResponseMessage<GetResponseData>, peer: PeerId) {
        let ResponseMessage { header, data } = response;

        let operation_id = header.operation_id;
        let is_ack = header.message_type == ResponseMessageType::Ack;

        match &data {
            GetResponseData::Data {
                assertion,
                metadata,
            } => {
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
                    // Retrieve operation context for validation from new service
                    match self.get_operation_service.get_context(&operation_id) {
                        Some(state) => {
                            // Build a ParsedUal from the stored state for validation
                            // Note: We don't have the contract address stored, but validation
                            // only needs blockchain, knowledge_collection_id, and
                            // knowledge_asset_id
                            let parsed_ual = ParsedUal {
                                blockchain: state.blockchain.clone(),
                                // Use a dummy contract address - validation doesn't use it
                                contract: blockchain::Address::ZERO,
                                knowledge_collection_id: state.knowledge_collection_id,
                                knowledge_asset_id: state.knowledge_asset_id,
                            };

                            let is_valid = self
                                .get_validation_service
                                .validate_response(
                                    &assertion.public,
                                    assertion.private.as_deref(),
                                    &parsed_ual,
                                    state.visibility,
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

        // If successful, store the result before recording response
        // (record_response may trigger completion status update)
        // If storing fails, treat as unsuccessful response
        let is_success = if is_success
            && let GetResponseData::Data {
                assertion,
                metadata,
            } = &data
        {
            let get_result = GetOperationResult::new(
                Assertion::new(assertion.public.clone(), assertion.private.clone()),
                metadata.clone(),
            );

            match self
                .get_operation_service
                .store_result(operation_id, &get_result)
            {
                Ok(()) => true,
                Err(e) => {
                    tracing::error!(
                        operation_id = %operation_id,
                        error = %e,
                        "Failed to store get result, treating as failed response"
                    );
                    false
                }
            }
        } else {
            is_success
        };

        // Record response using operation service (triggers completion signaling)
        if let Err(e) = self
            .get_operation_service
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
            self.get_operation_service.remove_context(&operation_id);
        }
    }
}
