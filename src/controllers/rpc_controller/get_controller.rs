use std::sync::Arc;

use crate::context::Context;
use crate::controllers::rpc_controller::base_controller::BaseController;
use async_trait::async_trait;
use network::action::NetworkAction;
use network::message::{
    GetInitResponseData, GetMessageRequestData, GetMessageResponseData, GetRequestResponseData,
    RequestMessage, RequestMessageHeader, RequestMessageType, ResponseMessage,
    ResponseMessageHeader, ResponseMessageType,
};

use network::{request_response, PeerId};
use tokio::sync::mpsc;

pub struct GetController {
    network_action_tx: mpsc::Sender<NetworkAction>,
}

#[async_trait]
impl BaseController for GetController {
    type RequestData = GetMessageRequestData;
    type ResponseData = GetMessageResponseData;

    fn new(context: Arc<Context>) -> Self {
        Self {
            network_action_tx: context.network_action_tx().clone(),
        }
    }

    async fn handle_request(
        &self,
        request: RequestMessage<Self::RequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<Self::ResponseData>>,
        peer: PeerId,
    ) {
        let RequestMessage { header, data } = request;

        let response_data = match data {
            GetMessageRequestData::Init(_) => {
                println!("Handling GET_INIT...");
                // TODO: handle GET_INIT
                GetMessageResponseData::InitResponse(GetInitResponseData::new(None))
            }
            GetMessageRequestData::Request(_) => {
                println!("Handling GET_REQUEST...");
                // TODO: handle GET_REQUEST
                let nquads = vec![];
                GetMessageResponseData::RequestResponse(GetRequestResponseData::new(nquads, None))
            }
        };

        let message = ResponseMessage {
            header: ResponseMessageHeader {
                operation_id: header.operation_id,
                keyword_uuid: header.keyword_uuid,
                message_type: ResponseMessageType::Ack,
            },
            data: response_data,
        };

        self.network_action_tx
            .send(NetworkAction::GetResponse { channel, message })
            .await
            .unwrap();
    }

    async fn handle_response(&self, response: ResponseMessage<Self::ResponseData>, peer: PeerId) {
        let ResponseMessage { data, .. } = response;

        match data {
            GetMessageResponseData::InitResponse(_) => {
                println!("Handling GET_INIT response...");
                // TODO: handle GET_INIT response
            }
            GetMessageResponseData::RequestResponse(_) => {
                println!("Handling GET_REQUEST response...");
                // TODO: handle GET_REQUEST response
            }
        }
    }
}
