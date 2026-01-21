use std::sync::Arc;

use network::{
    PeerId,
    message::{RequestMessage, ResponseMessage, ResponseMessageType},
    request_response,
};
use tokio::sync::mpsc::Sender;
use triple_store::Assertion;

use crate::{
    commands::{
        command_executor::CommandExecutionRequest, command_registry::Command,
        operations::publish::protocols::store::handle_store_request_command::HandleStoreRequestCommandData,
    },
    context::Context,
    controllers::rpc_controller::messages::{StoreRequestData, StoreResponseData},
    operations::{PublishOperation, PublishOperationResult, SignatureData},
    services::{
        ResponseChannels,
        operation::OperationService as GenericOperationService,
    },
};

pub struct StoreRpcController {
    publish_operation_service: Arc<GenericOperationService<PublishOperation>>,
    response_channels: Arc<ResponseChannels<StoreResponseData>>,
    schedule_command_tx: Sender<CommandExecutionRequest>,
}

impl StoreRpcController {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            publish_operation_service: Arc::clone(context.publish_operation_service()),
            response_channels: Arc::clone(context.store_response_channels()),
            schedule_command_tx: context.schedule_command_tx().clone(),
        }
    }

    /// Returns a reference to the operation service for this controller.
    pub fn operation_service(&self) -> &Arc<GenericOperationService<PublishOperation>> {
        &self.publish_operation_service
    }

    pub async fn handle_request(
        &self,
        request: RequestMessage<StoreRequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<StoreResponseData>>,
        remote_peer_id: PeerId,
    ) {
        let RequestMessage { header, data } = request;

        let operation_id = header.operation_id;

        tracing::trace!(
            operation_id = %operation_id,
            dataset_root = %data.dataset_root(),
            peer = %remote_peer_id,
            "Store request received"
        );

        // Store channel for later retrieval by command handler
        self.response_channels
            .store(&remote_peer_id, operation_id, channel);

        // Create dataset from request data
        let dataset = Assertion {
            public: data.dataset().to_owned(),
            private: None,
        };

        // Schedule command with dataset passed inline
        let command = Command::HandleStoreRequest(HandleStoreRequestCommandData::new(
            data.blockchain().clone(),
            operation_id,
            data.dataset_root().to_owned(),
            remote_peer_id,
            dataset,
        ));

        let request = CommandExecutionRequest::new(command);

        let command_name = request.command.name();
        if let Err(e) = self.schedule_command_tx.send(request).await {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                command_name = %command_name,
                "Failed to schedule command."
            );
        }
    }

    pub async fn handle_response(
        &self,
        response: ResponseMessage<StoreResponseData>,
        _peer: PeerId,
    ) {
        let ResponseMessage { header, data } = response;

        let operation_id = header.operation_id;
        let is_success = header.message_type == ResponseMessageType::Ack;

        // If successful, store signature immediately to redb
        if let (
            ResponseMessageType::Ack,
            StoreResponseData::Data {
                identity_id,
                signature,
            },
        ) = (header.message_type, &data)
        {
            let sig_data = SignatureData::new(
                identity_id.to_string(),
                signature.v,
                signature.r.clone(),
                signature.s.clone(),
                signature.vs.clone(),
            );

            // Store signature incrementally to redb
            if let Err(e) = self.publish_operation_service.update_result(
                operation_id,
                PublishOperationResult::new(None, Vec::new()),
                |result| {
                    result.network_signatures.push(sig_data);
                },
            ) {
                tracing::error!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to store network signature to redb"
                );
            }
        }

        // Record response using operation service (triggers completion signaling)
        if let Err(e) = self
            .publish_operation_service
            .record_response(operation_id, is_success)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to record response"
            );
        } else {
            tracing::debug!(
                operation_id = %operation_id,
                success = is_success,
                "Store response processed"
            );
        }
    }
}
