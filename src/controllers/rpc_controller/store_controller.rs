use std::sync::Arc;

use crate::context::Context;
use crate::controllers::rpc_controller::base_controller::BaseController;
use async_trait::async_trait;
use network::message::{
    MessageHeader, MessageType, RequestMessage, ResponseMessage, StoreInitResponseData,
    StoreMessageRequestData, StoreMessageResponseData, StoreRequestResponseData,
};
use network::{request_response, PeerId};

pub struct StoreController {
    context: Arc<Context>,
}

#[async_trait]
impl BaseController for StoreController {
    type RequestData = StoreMessageRequestData;
    type ResponseData = StoreMessageResponseData;

    fn new(context: Arc<Context>) -> Self {
        Self { context }
    }

    async fn handle_request(
        &self,
        request: RequestMessage<Self::RequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<Self::ResponseData>>,
        peer: PeerId,
    ) {
        let RequestMessage { header, data } = request;

        let response_data = match data {
            StoreMessageRequestData::Init(_) => {
                println!("Handling STORE_INIT...");
                // TODO: handle STORE_INIT
                StoreMessageResponseData::InitResponse(StoreInitResponseData::new(None))
            }
            StoreMessageRequestData::Request(_) => {
                println!("Handling STORE_REQUEST...");
                // TODO: handle STORE_REQUEST
                StoreMessageResponseData::RequestResponse(StoreRequestResponseData::new(None))
            }
        };

        let message = ResponseMessage {
            header: MessageHeader {
                operation_id: header.operation_id,
                keyword_uuid: header.keyword_uuid,
                message_type: MessageType::Ack,
            },
            data: response_data,
        };

        self.context
            .network_action_tx()
            .send(network::action::NetworkAction::StoreResponse { channel, message })
            .await
            .unwrap();
    }

    async fn handle_response(&self, response: ResponseMessage<Self::ResponseData>, peer: PeerId) {
        let ResponseMessage { data, .. } = response;

        match data {
            StoreMessageResponseData::InitResponse(_) => {
                println!("Handling STORE_INIT response...");
                // TODO: handle STORE_INIT response
            }
            StoreMessageResponseData::RequestResponse(_) => {
                println!("Handling STORE_REQUEST response...");
                // TODO: handle STORE_REQUEST response
            }
        }
    }
}
