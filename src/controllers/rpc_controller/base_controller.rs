use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use network::message::{RequestMessage, ResponseMessage};
use network::{request_response, PeerId};
use tracing::info;

use crate::context::Context;

#[async_trait]
pub trait BaseController {
    type RequestData: Send + Debug;
    type ResponseData: Send + Debug;

    fn new(context: Arc<Context>) -> Self;

    async fn handle_request(
        &self,
        request: RequestMessage<Self::RequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<Self::ResponseData>>,
        peer: PeerId,
    );

    async fn handle_response(&self, response: ResponseMessage<Self::ResponseData>, peer: PeerId);

    async fn handle_message(
        &self,
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
                self.handle_request(request, channel, peer).await;
            }
            request_response::Message::Response { response, .. } => {
                info!("Received response: {:?} from peer: {:?}", response, peer);
                self.handle_response(response, peer).await;
            }
        }
    }
}
