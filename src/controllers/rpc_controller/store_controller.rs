use std::sync::Arc;

use crate::context::Context;
use crate::controllers::rpc_controller::base_controller::BaseController;
use crate::services::operation_service::NetworkOperationProtocol;
use crate::services::publish_service::PublishService;
use async_trait::async_trait;
use network::action::NetworkAction;
use network::message::{
    RequestMessage, ResponseMessage, ResponseMessageHeader, ResponseMessageType, StoreRequestData,
    StoreResponseData,
};
use network::{request_response, PeerId};
use tokio::sync::mpsc;

pub struct StoreController {
    network_action_tx: mpsc::Sender<NetworkAction>,
    publish_service: Arc<PublishService>,
}

#[async_trait]
impl BaseController for StoreController {
    type RequestData = StoreRequestData;
    type ResponseData = StoreResponseData;

    fn new(context: Arc<Context>) -> Self {
        Self {
            network_action_tx: context.network_action_tx().clone(),
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

        self.network_action_tx
            .send(NetworkAction::StoreResponse { channel, message })
            .await
            .unwrap();
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
