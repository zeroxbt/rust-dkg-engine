use crate::controllers::rpc::base_controller::BaseController;
use async_trait::async_trait;
use network::message::{
    GetInitResponseData, GetMessageRequestData, GetMessageResponseData, GetRequestResponseData,
    MessageHeader, MessageType, RequestMessage, ResponseMessage,
};
use network::{command::NetworkCommand, request_response, PeerId};
use tokio::sync::mpsc::Sender;

pub struct GetController;

#[async_trait]
impl BaseController for GetController {
    type RequestData = GetMessageRequestData;
    type ResponseData = GetMessageResponseData;

    async fn handle_request(
        &self,
        network_command_tx: &Sender<NetworkCommand>,
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
            header: MessageHeader {
                operation_id: header.operation_id,
                keyword_uuid: header.keyword_uuid,
                message_type: MessageType::Ack,
            },
            data: response_data,
        };

        network_command_tx
            .send(network::command::NetworkCommand::GetResponse { channel, message })
            .await
            .unwrap();
    }

    async fn handle_response(
        &self,
        network_command_tx: &Sender<NetworkCommand>,
        response: ResponseMessage<Self::ResponseData>,
        peer: PeerId,
    ) {
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
