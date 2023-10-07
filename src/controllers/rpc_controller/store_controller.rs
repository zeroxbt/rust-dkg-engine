use std::sync::Arc;

use crate::context::Context;
use crate::controllers::rpc_controller::base_controller::BaseController;
use async_trait::async_trait;
use network::action::NetworkAction;
use network::message::{
    RequestMessage, ResponseMessage, ResponseMessageHeader, ResponseMessageType,
    StoreInitResponseData, StoreMessageRequestData, StoreMessageResponseData,
    StoreRequestResponseData,
};
use network::{request_response, PeerId};
use tokio::sync::mpsc;

pub struct StoreController {
    network_action_tx: mpsc::Sender<NetworkAction>,
}

#[async_trait]
impl BaseController for StoreController {
    type RequestData = StoreMessageRequestData;
    type ResponseData = StoreMessageResponseData;

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
            header: ResponseMessageHeader {
                operation_id: header.operation_id,
                keyword_uuid: header.keyword_uuid,
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

        match (header.message_type, data) {
            (ResponseMessageType::Nack, _) => {
                tracing::warn!("Received NACK from peer: {}", peer);
            }
            (ResponseMessageType::Busy, _) => {
                tracing::debug!("Peer: {} is BUSY", peer);
            }
            (ResponseMessageType::Ack, StoreMessageResponseData::InitResponse(_)) => {
                tracing::trace!(
                    "Received ACK response from peer: {} to STORE INIT message",
                    peer
                );
            }
            (ResponseMessageType::Ack, StoreMessageResponseData::RequestResponse(_)) => {
                tracing::trace!(
                    "Received ACK response from peer: {} to STORE REQUEST message",
                    peer
                );
            }
        }
    }
}
