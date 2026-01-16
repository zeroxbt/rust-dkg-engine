use std::sync::Arc;

use network::{
    PeerId,
    message::{RequestMessage, ResponseMessage, ResponseMessageType},
    request_response,
};
use repository::RepositoryManager;
use tokio::sync::mpsc::Sender;

use crate::{
    commands::{
        command::{Command, CommandBuilder},
        protocols::publish::handle_publish_request_command::HandlePublishRequestCommandData,
    },
    context::Context,
    network::SessionManager,
    services::operation_manager::OperationManager,
    types::{
        models::Assertion,
        protocol::{StoreRequestData, StoreResponseData},
    },
};

pub struct StoreRpcController {
    publish_operation_manager: Arc<OperationManager>,
    repository_manager: Arc<RepositoryManager>,
    session_manager: Arc<SessionManager<StoreResponseData>>,
    schedule_command_tx: Sender<Command>,
}

impl StoreRpcController {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            publish_operation_manager: Arc::clone(context.publish_operation_manager()),
            session_manager: Arc::clone(context.store_session_manager()),
            schedule_command_tx: context.schedule_command_tx().clone(),
        }
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

        // Store channel in session manager for later retrieval by command handler
        self.session_manager
            .store_channel(&remote_peer_id, operation_id, channel);

        // Create dataset from request data
        let dataset = Assertion {
            public: data.dataset().to_owned(),
            private: None,
        };

        // Schedule command with dataset passed inline
        let command = CommandBuilder::new("handlePublishRequestCommand")
            .data(HandlePublishRequestCommandData::new(
                data.blockchain().clone(),
                operation_id,
                data.dataset_root().to_owned(),
                remote_peer_id,
                dataset,
            ))
            .build();

        let command_name = command.name.clone();
        if let Err(e) = self.schedule_command_tx.send(command).await {
            tracing::error!(operation_id = %operation_id, error = %e, command_name = %command_name, "Failed to schedule command.");
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

        if let (
            ResponseMessageType::Ack,
            StoreResponseData::Data {
                identity_id,
                signature,
            },
        ) = (header.message_type, data)
            && let Err(e) = self
                .repository_manager
                .signature_repository()
                .store_network_signature(
                    header.operation_id,
                    &identity_id.to_string(),
                    signature.v,
                    &signature.r,
                    &signature.s,
                    &signature.vs,
                )
                .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to store network signature"
            );
        };

        // Record response using operation manager
        if let Err(e) = self
            .publish_operation_manager
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
