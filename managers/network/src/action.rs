use libp2p::{Multiaddr, PeerId, request_response::ResponseChannel};

use crate::message::{
    GetRequestData, GetResponseData, RequestMessage, ResponseMessage, StoreRequestData,
    StoreResponseData,
};

pub enum NetworkAction {
    StoreRequest {
        peer: PeerId,
        message: RequestMessage<StoreRequestData>,
    },
    StoreResponse {
        channel: ResponseChannel<ResponseMessage<StoreResponseData>>,
        message: ResponseMessage<StoreResponseData>,
    },
    GetRequest {
        peer: PeerId,
        message: RequestMessage<GetRequestData>,
    },
    GetResponse {
        channel: ResponseChannel<ResponseMessage<GetResponseData>>,
        message: ResponseMessage<GetResponseData>,
    },
    GetClosestPeers {
        peer: PeerId,
    },
    AddAddress {
        peer_id: PeerId,
        addresses: Vec<Multiaddr>,
    },
}
