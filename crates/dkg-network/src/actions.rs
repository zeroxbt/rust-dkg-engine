//! Network action commands sent to the swarm event loop.

use libp2p::{Multiaddr, PeerId, request_response};
use tokio::sync::oneshot;
use uuid::Uuid;

use super::{
    NetworkError,
    message::ResponseMessage,
    protocols::{
        BatchGetAck, BatchGetRequestData, BatchGetResponseData, FinalityAck, FinalityRequestData,
        FinalityResponseData, GetAck, GetRequestData, GetResponseData, StoreAck, StoreRequestData,
        StoreResponseData,
    },
};

/// Control-plane actions sent to the swarm event loop.
pub enum NetworkControlAction {
    /// Discover peers via Kademlia DHT lookup (get_closest_peers).
    /// This queries the network for nodes closest to the given peer IDs,
    /// which populates the routing table with their addresses.
    FindPeers(Vec<PeerId>),
    /// Get the list of currently connected peers.
    GetConnectedPeers(oneshot::Sender<Vec<PeerId>>),
    /// Add known addresses for peers to the Kademlia routing table.
    AddAddresses {
        addresses: Vec<(PeerId, Vec<Multiaddr>)>,
    },
}

impl NetworkControlAction {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::FindPeers(_) => "find_peers",
            Self::GetConnectedPeers(_) => "get_connected_peers",
            Self::AddAddresses { .. } => "add_addresses",
        }
    }
}

/// Data-plane protocol actions sent to the swarm event loop.
pub enum NetworkDataAction {
    // Protocol-specific request actions
    // The response_tx is stored in PendingRequests and used to deliver the response directly
    StoreRequest {
        peer: PeerId,
        operation_id: Uuid,
        request_data: StoreRequestData,
        response_tx: oneshot::Sender<Result<StoreResponseData, NetworkError>>,
    },
    GetRequest {
        peer: PeerId,
        operation_id: Uuid,
        request_data: GetRequestData,
        response_tx: oneshot::Sender<Result<GetResponseData, NetworkError>>,
    },
    FinalityRequest {
        peer: PeerId,
        operation_id: Uuid,
        request_data: FinalityRequestData,
        response_tx: oneshot::Sender<Result<FinalityResponseData, NetworkError>>,
    },
    BatchGetRequest {
        peer: PeerId,
        operation_id: Uuid,
        request_data: BatchGetRequestData,
        response_tx: oneshot::Sender<Result<BatchGetResponseData, NetworkError>>,
    },
    // Protocol-specific response actions
    StoreResponse {
        channel: request_response::ResponseChannel<ResponseMessage<StoreAck>>,
        message: ResponseMessage<StoreAck>,
    },
    GetResponse {
        channel: request_response::ResponseChannel<ResponseMessage<GetAck>>,
        message: ResponseMessage<GetAck>,
    },
    FinalityResponse {
        channel: request_response::ResponseChannel<ResponseMessage<FinalityAck>>,
        message: ResponseMessage<FinalityAck>,
    },
    BatchGetResponse {
        channel: request_response::ResponseChannel<ResponseMessage<BatchGetAck>>,
        message: ResponseMessage<BatchGetAck>,
    },
}

impl NetworkDataAction {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::StoreRequest { .. } => "store_request",
            Self::GetRequest { .. } => "get_request",
            Self::FinalityRequest { .. } => "finality_request",
            Self::BatchGetRequest { .. } => "batch_get_request",
            Self::StoreResponse { .. } => "store_response",
            Self::GetResponse { .. } => "get_response",
            Self::FinalityResponse { .. } => "finality_response",
            Self::BatchGetResponse { .. } => "batch_get_response",
        }
    }
}
