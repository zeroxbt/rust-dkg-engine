use async_trait::async_trait;
use network::message::{RequestMessage, ResponseMessage};
use network::{command::NetworkCommand, request_response, PeerId};
use tokio::sync::mpsc::Sender;
use tracing::info;

#[async_trait]
pub trait BaseHandler
where
    Self::RequestData: std::fmt::Debug,
    Self::ResponseData: std::fmt::Debug,
{
    type RequestData: Send;
    type ResponseData: Send;

    async fn handle_request(
        &self,
        network_manager: &Sender<NetworkCommand>,
        request: RequestMessage<Self::RequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<Self::ResponseData>>,
        peer: PeerId,
    );

    async fn handle_response(
        &self,
        network_manager: &Sender<NetworkCommand>,
        response: ResponseMessage<Self::ResponseData>,
        peer: PeerId,
    );

    async fn handle_message(
        &self,
        network_manager: &Sender<NetworkCommand>,
        message: request_response::Message<
            RequestMessage<Self::RequestData>,
            ResponseMessage<Self::ResponseData>,
        >,
        peer: PeerId,
    ) {
        match message {
            request_response::Message::Request {
                request, channel, ..
            } => {
                info!("Received message: {:?} from peer: {:?}", request, peer);
                self.handle_request(network_manager, request, channel, peer)
                    .await;
            }
            request_response::Message::Response { response, .. } => {
                info!("Received response: {:?} from peer: {:?}", response, peer);
                self.handle_response(network_manager, response, peer).await;
            }
        }
    }
}
