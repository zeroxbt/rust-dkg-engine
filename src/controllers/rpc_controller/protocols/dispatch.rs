use super::behaviour::NetworkProtocols;
use crate::{
    controllers::rpc_controller::messages::{
        BatchGetRequestData, BatchGetResponseData, FinalityRequestData, FinalityResponseData,
        GetRequestData, GetResponseData, StoreRequestData, StoreResponseData,
    },
    managers::network::{
        Multiaddr, PeerId, ProtocolDispatch, RequestMessage, ResponseMessage, request_response,
    },
};

pub(crate) enum ProtocolRequest {
    Store {
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        message: RequestMessage<StoreRequestData>,
    },
    Get {
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        message: RequestMessage<GetRequestData>,
    },
    Finality {
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        message: RequestMessage<FinalityRequestData>,
    },
    BatchGet {
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        message: RequestMessage<BatchGetRequestData>,
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
    BatchGet {
        channel: request_response::ResponseChannel<ResponseMessage<BatchGetResponseData>>,
        message: ResponseMessage<BatchGetResponseData>,
    },
}

impl ProtocolDispatch for NetworkProtocols {
    type Request = ProtocolRequest;
    type Response = ProtocolResponse;

    fn send_request(&mut self, request: Self::Request) -> request_response::OutboundRequestId {
        match request {
            ProtocolRequest::Store {
                peer,
                addresses,
                message,
            } => self
                .store
                .send_request_with_addresses(&peer, message, addresses),
            ProtocolRequest::Get {
                peer,
                addresses,
                message,
            } => self
                .get
                .send_request_with_addresses(&peer, message, addresses),
            ProtocolRequest::Finality {
                peer,
                addresses,
                message,
            } => self
                .finality
                .send_request_with_addresses(&peer, message, addresses),
            ProtocolRequest::BatchGet {
                peer,
                addresses,
                message,
            } => self
                .batch_get
                .send_request_with_addresses(&peer, message, addresses),
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
            ProtocolResponse::BatchGet { channel, message } => {
                let _ = self.batch_get.send_response(channel, message);
            }
        }
    }
}
