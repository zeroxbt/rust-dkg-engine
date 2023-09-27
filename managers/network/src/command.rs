use crate::message::{
    GetMessageRequestData, GetMessageResponseData, RequestMessage, ResponseMessage,
    StoreMessageRequestData, StoreMessageResponseData,
};
use libp2p::request_response::ResponseChannel;
use libp2p::{Multiaddr, PeerId};

pub enum NetworkCommand {
    StoreRequest {
        peer: PeerId,
        message: RequestMessage<StoreMessageRequestData>,
    },
    StoreResponse {
        channel: ResponseChannel<ResponseMessage<StoreMessageResponseData>>,
        message: ResponseMessage<StoreMessageResponseData>,
    },
    GetRequest {
        peer: PeerId,
        message: RequestMessage<GetMessageRequestData>,
    },
    GetResponse {
        channel: ResponseChannel<ResponseMessage<GetMessageResponseData>>,
        message: ResponseMessage<GetMessageResponseData>,
    },
    GetClosestPeers {
        peer: PeerId,
    },
    AddAddress {
        peer_id: PeerId,
        addresses: Vec<Multiaddr>,
    },
}
/*
impl NetworkCommand<StoreMessageRequestData, StoreMessageResponseData> {
    pub fn new_store_request(
        peer: PeerId,
        message: RequestMessage<StoreMessageRequestData>,
    ) -> Self {
        NetworkCommand::StoreRequest {
            peer,
            message,
            protocol: StreamProtocol::new("/store/1.0.0"),
        }
    }

    pub fn new_store_response(
        channel: ResponseChannel<ResponseMessage<StoreMessageResponseData>>,
        message: ResponseMessage<StoreMessageResponseData>,
    ) -> Self {
        NetworkCommand::StoreResponse { channel, message }
    }
} */
