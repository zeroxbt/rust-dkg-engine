use std::sync::Arc;

use network::{
    NetworkManager, PeerId,
    message::{RequestMessage, ResponseMessage, ResponseMessageHeader, ResponseMessageType},
    request_response,
};

use crate::{
    context::Context,
    network::{NetworkProtocols, ProtocolResponse},
    services::publish_service::PublishService,
    types::{
        protocol::{StoreRequestData, StoreResponseData},
        traits::{controller::BaseController, service::NetworkOperationProtocol},
    },
};

pub struct StoreController {
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    publish_service: Arc<PublishService>,
}

impl BaseController for StoreController {
    type RequestData = StoreRequestData;
    type ResponseData = StoreResponseData;

    fn new(context: Arc<Context>) -> Self {
        Self {
            network_manager: Arc::clone(context.network_manager()),
            publish_service: Arc::clone(context.publish_service()),
        }
    }

    async fn handle_request(
        &self,
        request: RequestMessage<Self::RequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<Self::ResponseData>>,
        peer: PeerId,
    ) {
        let RequestMessage { header, data } = request;

        let response_data = {
            println!("Handling STORE request...");
            // TODO: handle STORE request
            let s = String::new();
            StoreResponseData::new(s.clone(), 0, s.clone(), s.clone(), s, None)
        };

        let message = ResponseMessage {
            header: ResponseMessageHeader {
                operation_id: header.operation_id,
                message_type: ResponseMessageType::Ack,
            },
            data: response_data,
        };

        // Send response directly through swarm
        let _ = self
            .network_manager
            .send_protocol_response(ProtocolResponse::store(channel, message))
            .await;
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
