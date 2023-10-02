use crate::handlers::rpc_handler::base_handler::BaseHandler;
use async_trait::async_trait;
use network::message::{
    MessageHeader, MessageType, RequestMessage, ResponseMessage, StoreInitResponseData,
    StoreMessageRequestData, StoreMessageResponseData, StoreRequestResponseData,
};
use network::{command::NetworkCommand, request_response, PeerId};
use tokio::sync::mpsc::Sender;

pub struct StoreHandler;

#[async_trait]
impl BaseHandler for StoreHandler {
    type RequestData = StoreMessageRequestData;
    type ResponseData = StoreMessageResponseData;

    async fn handle_request(
        &self,
        network_command_tx: &Sender<NetworkCommand>,
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

        network_command_tx
            .send(network::command::NetworkCommand::StoreResponse { channel, message })
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
