/* use std::sync::Arc;

use network::{
    ErrorMessage, NetworkManager, PeerId,
    message::{RequestMessage, ResponseMessage, ResponseMessageHeader, ResponseMessageType},
    request_response,
};

use crate::{
    context::Context,
    network::{NetworkProtocols, ProtocolResponse},
    types::{
        protocol::{GetRequestData, GetResponseData},
        traits::controller::BaseController,
    },
};

pub struct GetController {
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
}

impl BaseController for GetController {
    type RequestData = GetRequestData;
    type ResponseData = GetResponseData;

    fn new(context: Arc<Context>) -> Self {
        Self {
            network_manager: Arc::clone(context.network_manager()),
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
            println!("Handling GET request...");
            // TODO: handle GET request
            GetResponseData::Error {
                error_message: "unimplemented".to_string(),
            }
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
            .send_protocol_response(ProtocolResponse::get(channel, message))
            .await;
    }

    async fn handle_response(&self, response: ResponseMessage<Self::ResponseData>, peer: PeerId) {
        let ResponseMessage { data, .. } = response;

        println!("Handling GET response...");
    }
}
 */
