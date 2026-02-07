//! NetworkManager handle - lightweight API for network operations.
//!
//! This is the public-facing API that callers use to interact with the network.
//! It communicates with the NetworkEventLoop via an action channel.

use libp2p::{Multiaddr, PeerId, identity, request_response};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::instrument;
use uuid::Uuid;

use super::{
    NetworkError, NetworkManagerConfig, ResponseMessage,
    actions::NetworkAction,
    behaviour::build_swarm,
    protocols::{
        BatchGetAck, BatchGetRequestData, BatchGetResponseData, FinalityAck, FinalityRequestData,
        FinalityResponseData, GetAck, GetRequestData, GetResponseData, StoreAck, StoreRequestData,
        StoreResponseData,
    },
};

/// NetworkManager handle for network operations.
///
/// This is a lightweight handle that sends actions to the NetworkEventLoop.
/// All network operations go through the action channel to the event loop.
pub(crate) struct NetworkManager {
    action_tx: mpsc::Sender<NetworkAction>,
    peer_id: PeerId,
    peer_event_tx: broadcast::Sender<super::PeerEvent>,
}

impl NetworkManager {
    /// Creates a new NetworkManager handle and NetworkEventLoop pair.
    ///
    /// # Arguments
    /// * `config` - Network configuration (port, bootstrap nodes)
    /// * `key` - Pre-loaded libp2p identity keypair
    ///
    /// # Returns
    /// A tuple of (NetworkManager handle, NetworkEventLoop) on success.
    ///
    /// # Errors
    /// Returns `NetworkError` if swarm building fails.
    pub(crate) fn connect(
        config: &NetworkManagerConfig,
        key: identity::Keypair,
    ) -> Result<(Self, super::event_loop::NetworkEventLoop), NetworkError> {
        let (swarm, local_peer_id) = build_swarm(config, key)?;
        let (action_tx, action_rx) = mpsc::channel(128);
        let (peer_event_tx, _) = broadcast::channel(1024);

        let handle = Self {
            action_tx,
            peer_id: local_peer_id,
            peer_event_tx: peer_event_tx.clone(),
        };

        let event_loop = super::event_loop::NetworkEventLoop::new(
            swarm,
            action_rx,
            config.clone(),
            peer_event_tx,
        );

        Ok((handle, event_loop))
    }

    /// Returns the peer ID of this node.
    pub(crate) fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    pub(crate) fn subscribe_peer_events(&self) -> broadcast::Receiver<super::PeerEvent> {
        self.peer_event_tx.subscribe()
    }

    async fn enqueue_action(&self, action: NetworkAction) -> Result<(), NetworkError> {
        self.action_tx
            .send(action)
            .await
            .map_err(|_| NetworkError::ActionChannelClosed)
    }

    /// Discover peers via Kademlia DHT lookup.
    /// This initiates a get_closest_peers query for each peer, which will discover
    /// their addresses through the DHT and add them to the routing table.
    pub(crate) async fn find_peers(&self, peers: Vec<PeerId>) -> Result<(), NetworkError> {
        self.enqueue_action(NetworkAction::FindPeers(peers)).await
    }

    /// Add known addresses for peers to the Kademlia routing table.
    /// Used at startup to inject persisted peer addresses.
    pub(crate) async fn add_addresses(
        &self,
        addresses: Vec<(PeerId, Vec<Multiaddr>)>,
    ) -> Result<(), NetworkError> {
        self.enqueue_action(NetworkAction::AddAddresses { addresses })
            .await
    }

    /// Get the list of currently connected peers.
    pub(crate) async fn connected_peers(&self) -> Result<Vec<PeerId>, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_action(NetworkAction::GetConnectedPeers(tx))
            .await?;
        rx.await.map_err(|_| NetworkError::ResponseChannelClosed)
    }

    // Protocol-specific send methods that send a request and await the response.
    // These methods atomically register the pending request inside the network loop
    // BEFORE the request is sent, preventing race conditions with DialFailure/Timeout events.
    // The message wrapping (RequestMessage with header) is handled internally.

    /// Send a store request and await the response.
    #[instrument(
        name = "network_store",
        skip(self, request_data),
        fields(peer_id = %peer, operation_id = %operation_id)
    )]
    pub(crate) async fn send_store_request(
        &self,
        peer: PeerId,
        operation_id: Uuid,
        request_data: StoreRequestData,
    ) -> Result<StoreResponseData, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_action(NetworkAction::SendStoreRequest {
            peer,
            operation_id,
            request_data,
            response_tx: tx,
        })
        .await?;
        rx.await.map_err(|_| NetworkError::ResponseChannelClosed)?
    }

    /// Send a store response.
    pub(crate) async fn send_store_response(
        &self,
        channel: request_response::ResponseChannel<ResponseMessage<StoreAck>>,
        message: ResponseMessage<StoreAck>,
    ) -> Result<(), NetworkError> {
        self.enqueue_action(NetworkAction::SendStoreResponse { channel, message })
            .await
    }

    /// Send a get request and await the response.
    pub(crate) async fn send_get_request(
        &self,
        peer: PeerId,
        operation_id: Uuid,
        request_data: GetRequestData,
    ) -> Result<GetResponseData, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_action(NetworkAction::SendGetRequest {
            peer,
            operation_id,
            request_data,
            response_tx: tx,
        })
        .await?;
        rx.await.map_err(|_| NetworkError::ResponseChannelClosed)?
    }

    /// Send a get response.
    pub(crate) async fn send_get_response(
        &self,
        channel: request_response::ResponseChannel<ResponseMessage<GetAck>>,
        message: ResponseMessage<GetAck>,
    ) -> Result<(), NetworkError> {
        self.enqueue_action(NetworkAction::SendGetResponse { channel, message })
            .await
    }

    /// Send a finality request and await the response.
    pub(crate) async fn send_finality_request(
        &self,
        peer: PeerId,
        operation_id: Uuid,
        request_data: FinalityRequestData,
    ) -> Result<FinalityResponseData, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_action(NetworkAction::SendFinalityRequest {
            peer,
            operation_id,
            request_data,
            response_tx: tx,
        })
        .await?;
        rx.await.map_err(|_| NetworkError::ResponseChannelClosed)?
    }

    /// Send a finality response.
    pub(crate) async fn send_finality_response(
        &self,
        channel: request_response::ResponseChannel<ResponseMessage<FinalityAck>>,
        message: ResponseMessage<FinalityAck>,
    ) -> Result<(), NetworkError> {
        self.enqueue_action(NetworkAction::SendFinalityResponse { channel, message })
            .await
    }

    /// Send a batch get request and await the response.
    #[instrument(
        name = "network_batch_get",
        skip(self, request_data),
        fields(peer_id = %peer, operation_id = %operation_id)
    )]
    pub(crate) async fn send_batch_get_request(
        &self,
        peer: PeerId,
        operation_id: Uuid,
        request_data: BatchGetRequestData,
    ) -> Result<BatchGetResponseData, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_action(NetworkAction::SendBatchGetRequest {
            peer,
            operation_id,
            request_data,
            response_tx: tx,
        })
        .await?;
        rx.await.map_err(|_| NetworkError::ResponseChannelClosed)?
    }

    /// Send a batch get response.
    pub(crate) async fn send_batch_get_response(
        &self,
        channel: request_response::ResponseChannel<ResponseMessage<BatchGetAck>>,
        message: ResponseMessage<BatchGetAck>,
    ) -> Result<(), NetworkError> {
        self.enqueue_action(NetworkAction::SendBatchGetResponse { channel, message })
            .await
    }
}
