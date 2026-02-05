//! Network action commands sent to the swarm event loop.

use libp2p::{PeerId, request_response};
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

/// Network action commands sent to the swarm event loop.
pub(crate) enum NetworkAction {
    /// Discover peers via Kademlia DHT lookup (get_closest_peers).
    /// This queries the network for nodes closest to the given peer IDs,
    /// which populates the routing table with their addresses.
    FindPeers(Vec<PeerId>),
    /// Get the list of currently connected peers.
    GetConnectedPeers(oneshot::Sender<Vec<PeerId>>),
    // Protocol-specific request actions
    // The response_tx is stored in PendingRequests and used to deliver the response directly
    SendStoreRequest {
        peer: PeerId,
        operation_id: Uuid,
        request_data: StoreRequestData,
        response_tx: oneshot::Sender<Result<StoreResponseData, NetworkError>>,
    },
    SendGetRequest {
        peer: PeerId,
        operation_id: Uuid,
        request_data: GetRequestData,
        response_tx: oneshot::Sender<Result<GetResponseData, NetworkError>>,
    },
    SendFinalityRequest {
        peer: PeerId,
        operation_id: Uuid,
        request_data: FinalityRequestData,
        response_tx: oneshot::Sender<Result<FinalityResponseData, NetworkError>>,
    },
    SendBatchGetRequest {
        peer: PeerId,
        operation_id: Uuid,
        request_data: BatchGetRequestData,
        response_tx: oneshot::Sender<Result<BatchGetResponseData, NetworkError>>,
    },
    // Protocol-specific response actions
    SendStoreResponse {
        channel: request_response::ResponseChannel<ResponseMessage<StoreAck>>,
        message: ResponseMessage<StoreAck>,
    },
    SendGetResponse {
        channel: request_response::ResponseChannel<ResponseMessage<GetAck>>,
        message: ResponseMessage<GetAck>,
    },
    SendFinalityResponse {
        channel: request_response::ResponseChannel<ResponseMessage<FinalityAck>>,
        message: ResponseMessage<FinalityAck>,
    },
    SendBatchGetResponse {
        channel: request_response::ResponseChannel<ResponseMessage<BatchGetAck>>,
        message: ResponseMessage<BatchGetAck>,
    },
}
