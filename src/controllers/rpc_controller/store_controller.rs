use std::sync::Arc;

use network::{
    NetworkManager, PeerId,
    message::{RequestMessage, ResponseMessage, ResponseMessageType},
    request_response,
};

use crate::{
    commands::protocols::publish::handle_publish_request_command::HandlePublishRequestCommandData,
    context::Context,
    network::{NetworkProtocols, SessionManager},
    services::publish_service::PublishService,
    types::{
        models::OperationId,
        protocol::{StoreRequestData, StoreResponseData},
        traits::{
            command::CommandData, controller::BaseController, service::NetworkOperationProtocol,
        },
    },
};

pub struct StoreController {
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    publish_service: Arc<PublishService>,
    session_manager: Arc<SessionManager<StoreResponseData>>,
    context: Arc<Context>,
}

impl BaseController for StoreController {
    type RequestData = StoreRequestData;
    type ResponseData = StoreResponseData;

    fn new(context: Arc<Context>) -> Self {
        Self {
            network_manager: Arc::clone(context.network_manager()),
            publish_service: Arc::clone(context.publish_service()),
            session_manager: Arc::clone(context.store_session_manager()),
            context: Arc::clone(&context),
        }
    }

    async fn handle_request(
        &self,
        request: RequestMessage<Self::RequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<Self::ResponseData>>,
        peer: PeerId,
    ) {
        let RequestMessage { header, data } = request;

        let operation_id = OperationId::from(header.operation_id);

        tracing::debug!(
            "Received STORE request from peer: {}, operation_id: {}",
            peer,
            operation_id
        );

        // Cache the session channel for later use by the command
        self.session_manager
            .store_channel(peer, operation_id, channel);

        // Schedule the command to handle the request asynchronously
        let command = HandlePublishRequestCommandData::new(
            data.blockchain(),
            operation_id,
            data.dataset_root().to_string(),
            peer.to_string(),
        )
        .into_command();

        if let Err(e) = self.context.schedule_command_tx().send(command).await {
            tracing::error!(
                "Failed to schedule HandlePublishRequestCommand for operation {}: {}",
                operation_id,
                e
            );

            // Remove the cached session since we won't be processing it
            self.session_manager
                .remove_session(&peer.to_string(), operation_id);
        }
    }

    async fn handle_response(&self, response: ResponseMessage<Self::ResponseData>, peer: PeerId) {
        let ResponseMessage { header, data } = response;

        match (&header.message_type, data) {
            (ResponseMessageType::Ack, _)
            | (ResponseMessageType::Nack | ResponseMessageType::Busy, _) => {
                self.publish_service
                    .handle_request_response(header.operation_id, header.message_type)
                    .await;
            }
        }
    }
}
