use super::behaviour::NetworkProtocols;
use crate::{
    controllers::rpc_controller::messages::{
        FinalityRequestData, FinalityResponseData, GetRequestData, GetResponseData,
        StoreRequestData, StoreResponseData,
    },
    managers::network::{
        PeerId, ProtocolDispatch, RequestMessage, ResponseMessage, request_response,
    },
};

pub(crate) enum ProtocolRequest {
    Store {
        peer: PeerId,
        message: RequestMessage<StoreRequestData>,
    },
    Get {
        peer: PeerId,
        message: RequestMessage<GetRequestData>,
    },
    Finality {
        peer: PeerId,
        message: RequestMessage<FinalityRequestData>,
    },
}

pub(crate) enum ProtocolResponse {
    Store {
        channel: request_response::ResponseChannel<ResponseMessage<StoreResponseData>>,
        message: ResponseMessage<StoreResponseData>,
    },
    Get {
        channel: request_response::ResponseChannel<ResponseMessage<GetResponseData>>,
        message: ResponseMessage<GetResponseData>,
    },
    Finality {
        channel: request_response::ResponseChannel<ResponseMessage<FinalityResponseData>>,
        message: ResponseMessage<FinalityResponseData>,
    },
}

impl ProtocolDispatch for NetworkProtocols {
    type Request = ProtocolRequest;
    type Response = ProtocolResponse;

    fn send_request(&mut self, request: Self::Request) -> request_response::OutboundRequestId {
        match request {
            ProtocolRequest::Store { peer, message } => self.store.send_request(&peer, message),
            ProtocolRequest::Get { peer, message } => self.get.send_request(&peer, message),
            ProtocolRequest::Finality { peer, message } => {
                self.finality.send_request(&peer, message)
            }
        }
    }

    fn send_response(&mut self, response: Self::Response) {
        match response {
            ProtocolResponse::Store { channel, message } => {
                let _ = self.store.send_response(channel, message);
            }
            ProtocolResponse::Get { channel, message } => {
                let _ = self.get.send_response(channel, message);
            }
            ProtocolResponse::Finality { channel, message } => {
                let _ = self.finality.send_response(channel, message);
            }
        }
    }
}
