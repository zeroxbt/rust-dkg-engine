use network::{PeerId, ProtocolDispatch, RequestMessage, ResponseMessage, request_response};

use super::behaviour::NetworkProtocols;
use crate::types::protocol::{
    FinalityRequestData, FinalityResponseData, GetRequestData, GetResponseData, StoreRequestData,
    StoreResponseData,
};

pub enum ProtocolRequest {
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

impl ProtocolRequest {
    pub fn store(peer: PeerId, message: RequestMessage<StoreRequestData>) -> Self {
        Self::Store { peer, message }
    }

    pub fn get(peer: PeerId, message: RequestMessage<GetRequestData>) -> Self {
        Self::Get { peer, message }
    }

    pub fn finality(peer: PeerId, message: RequestMessage<FinalityRequestData>) -> Self {
        Self::Finality { peer, message }
    }
}

pub enum ProtocolResponse {
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

impl ProtocolResponse {
    pub fn store(
        channel: request_response::ResponseChannel<ResponseMessage<StoreResponseData>>,
        message: ResponseMessage<StoreResponseData>,
    ) -> Self {
        Self::Store { channel, message }
    }

    pub fn get(
        channel: request_response::ResponseChannel<ResponseMessage<GetResponseData>>,
        message: ResponseMessage<GetResponseData>,
    ) -> Self {
        Self::Get { channel, message }
    }

    pub fn finality(
        channel: request_response::ResponseChannel<ResponseMessage<FinalityResponseData>>,
        message: ResponseMessage<FinalityResponseData>,
    ) -> Self {
        Self::Finality { channel, message }
    }
}

impl ProtocolDispatch for NetworkProtocols {
    type Request = ProtocolRequest;
    type Response = ProtocolResponse;

    fn send_request(&mut self, request: Self::Request) {
        match request {
            ProtocolRequest::Store { peer, message } => {
                let _ = self.store.send_request(&peer, message);
            }
            ProtocolRequest::Get { peer, message } => {
                let _ = self.get.send_request(&peer, message);
            }
            ProtocolRequest::Finality { peer, message } => {
                let _ = self.finality.send_request(&peer, message);
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
