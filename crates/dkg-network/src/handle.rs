//! NetworkManager handle - lightweight API for network operations.
//!
//! This is the public-facing API that callers use to interact with the network.
//! It communicates with the NetworkEventLoop via an action channel.

use std::time::Instant;

use dkg_observability as observability;
use libp2p::{Multiaddr, PeerId, identity};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use uuid::Uuid;

use crate::{
    PROTOCOL_NAME_BATCH_GET, PROTOCOL_NAME_FINALITY, PROTOCOL_NAME_GET, PROTOCOL_NAME_STORE,
};

use super::{
    NetworkError, NetworkManagerConfig, ResponseHandle,
    actions::{NetworkControlAction, NetworkDataAction},
    behaviour::build_swarm,
    message::ResponseMessage,
    protocols::{
        BatchGetAck, BatchGetRequestData, BatchGetResponseData, FinalityAck, FinalityRequestData,
        FinalityResponseData, GetAck, GetRequestData, GetResponseData, StoreAck, StoreRequestData,
        StoreResponseData,
    },
};

pub(super) const ACTION_CHANNEL_CAPACITY: usize = 128;

/// NetworkManager handle for network operations.
///
/// This is a lightweight handle that sends actions to the NetworkEventLoop.
/// Control and protocol data actions are sent via separate channels.
pub struct NetworkManager {
    control_tx: mpsc::Sender<NetworkControlAction>,
    data_tx: mpsc::Sender<NetworkDataAction>,
    shutdown: CancellationToken,
    peer_id: PeerId,
    peer_event_tx: broadcast::Sender<super::PeerEvent>,
}

impl NetworkManager {
    async fn await_protocol_response<T>(
        protocol: &str,
        rx: oneshot::Receiver<Result<T, NetworkError>>,
    ) -> Result<T, NetworkError> {
        let started = Instant::now();
        match rx.await {
            Ok(result) => {
                observability::record_network_response_channel(protocol, "ok", started.elapsed());
                result
            }
            Err(_) => {
                observability::record_network_response_channel(
                    protocol,
                    "closed",
                    started.elapsed(),
                );
                Err(NetworkError::ResponseChannelClosed)
            }
        }
    }

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
    pub fn connect(
        config: &NetworkManagerConfig,
        key: identity::Keypair,
    ) -> Result<(Self, super::event_loop::NetworkEventLoop), NetworkError> {
        let (swarm, local_peer_id) = build_swarm(config, key)?;
        let (control_tx, control_rx) = mpsc::channel(ACTION_CHANNEL_CAPACITY);
        let (data_tx, data_rx) = mpsc::channel(ACTION_CHANNEL_CAPACITY);
        let (peer_event_tx, _) = broadcast::channel(1024);
        let shutdown = CancellationToken::new();

        let handle = Self {
            control_tx,
            data_tx,
            shutdown: shutdown.clone(),
            peer_id: local_peer_id,
            peer_event_tx: peer_event_tx.clone(),
        };

        let event_loop = super::event_loop::NetworkEventLoop::new(
            swarm,
            control_rx,
            data_rx,
            config.clone(),
            peer_event_tx,
            shutdown,
        );

        Ok((handle, event_loop))
    }

    /// Returns the peer ID of this node.
    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    pub fn subscribe_peer_events(&self) -> broadcast::Receiver<super::PeerEvent> {
        self.peer_event_tx.subscribe()
    }

    async fn enqueue_control_action(
        &self,
        action: NetworkControlAction,
    ) -> Result<(), NetworkError> {
        let action_kind = action.kind();
        let started = Instant::now();
        let result = self.control_tx.send(action).await;
        let status = if result.is_ok() { "ok" } else { "closed" };
        observability::record_network_action_enqueue(
            "control",
            action_kind,
            status,
            started.elapsed(),
        );
        result.map_err(|_| NetworkError::ActionChannelClosed)
    }

    async fn enqueue_data_action(&self, action: NetworkDataAction) -> Result<(), NetworkError> {
        let action_kind = action.kind();
        let started = Instant::now();
        let result = self.data_tx.send(action).await;
        let status = if result.is_ok() { "ok" } else { "closed" };
        observability::record_network_action_enqueue(
            "data",
            action_kind,
            status,
            started.elapsed(),
        );
        result.map_err(|_| NetworkError::ActionChannelClosed)
    }

    /// Gracefully stop the network event loop.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Discover peers via Kademlia DHT lookup.
    /// This initiates a get_closest_peers query for each peer, which will discover
    /// their addresses through the DHT and add them to the routing table.
    pub async fn find_peers(&self, peers: Vec<PeerId>) -> Result<(), NetworkError> {
        self.enqueue_control_action(NetworkControlAction::FindPeers(peers))
            .await
    }

    /// Add known addresses for peers to the Kademlia routing table.
    /// Used at startup to inject persisted peer addresses.
    pub async fn add_addresses(
        &self,
        addresses: Vec<(PeerId, Vec<Multiaddr>)>,
    ) -> Result<(), NetworkError> {
        self.enqueue_control_action(NetworkControlAction::AddAddresses { addresses })
            .await
    }

    /// Get the list of currently connected peers.
    pub async fn connected_peers(&self) -> Result<Vec<PeerId>, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_control_action(NetworkControlAction::GetConnectedPeers(tx))
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
    pub async fn send_store_request(
        &self,
        peer: PeerId,
        operation_id: Uuid,
        request_data: StoreRequestData,
    ) -> Result<StoreResponseData, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_data_action(NetworkDataAction::StoreRequest {
            peer,
            operation_id,
            request_data,
            response_tx: tx,
        })
        .await?;
        Self::await_protocol_response(PROTOCOL_NAME_STORE, rx).await
    }

    async fn send_store_response_message(
        &self,
        response: ResponseHandle<StoreAck>,
        message: ResponseMessage<StoreAck>,
    ) -> Result<(), NetworkError> {
        self.enqueue_data_action(NetworkDataAction::StoreResponse {
            channel: response.into_inner(),
            message,
        })
        .await
    }

    /// Send a store ACK response for an inbound request.
    pub async fn send_store_ack(
        &self,
        response: ResponseHandle<StoreAck>,
        operation_id: Uuid,
        ack: StoreAck,
    ) -> Result<(), NetworkError> {
        self.send_store_response_message(response, ResponseMessage::ack(operation_id, ack))
            .await
    }

    /// Send a store NACK response for an inbound request.
    pub async fn send_store_nack(
        &self,
        response: ResponseHandle<StoreAck>,
        operation_id: Uuid,
        error_message: impl Into<String>,
    ) -> Result<(), NetworkError> {
        self.send_store_response_message(
            response,
            ResponseMessage::nack(operation_id, error_message),
        )
        .await
    }

    /// Send a get request and await the response.
    #[instrument(
        name = "network_get",
        skip(self, request_data),
        fields(peer_id = %peer, operation_id = %operation_id)
    )]
    pub async fn send_get_request(
        &self,
        peer: PeerId,
        operation_id: Uuid,
        request_data: GetRequestData,
    ) -> Result<GetResponseData, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_data_action(NetworkDataAction::GetRequest {
            peer,
            operation_id,
            request_data,
            response_tx: tx,
        })
        .await?;
        Self::await_protocol_response(PROTOCOL_NAME_GET, rx).await
    }

    async fn send_get_response_message(
        &self,
        response: ResponseHandle<GetAck>,
        message: ResponseMessage<GetAck>,
    ) -> Result<(), NetworkError> {
        self.enqueue_data_action(NetworkDataAction::GetResponse {
            channel: response.into_inner(),
            message,
        })
        .await
    }

    /// Send a get ACK response for an inbound request.
    pub async fn send_get_ack(
        &self,
        response: ResponseHandle<GetAck>,
        operation_id: Uuid,
        ack: GetAck,
    ) -> Result<(), NetworkError> {
        self.send_get_response_message(response, ResponseMessage::ack(operation_id, ack))
            .await
    }

    /// Send a get NACK response for an inbound request.
    pub async fn send_get_nack(
        &self,
        response: ResponseHandle<GetAck>,
        operation_id: Uuid,
        error_message: impl Into<String>,
    ) -> Result<(), NetworkError> {
        self.send_get_response_message(response, ResponseMessage::nack(operation_id, error_message))
            .await
    }

    /// Send a finality request and await the response.
    #[instrument(
        name = "network_finality",
        skip(self, request_data),
        fields(peer_id = %peer, operation_id = %operation_id)
    )]
    pub async fn send_finality_request(
        &self,
        peer: PeerId,
        operation_id: Uuid,
        request_data: FinalityRequestData,
    ) -> Result<FinalityResponseData, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_data_action(NetworkDataAction::FinalityRequest {
            peer,
            operation_id,
            request_data,
            response_tx: tx,
        })
        .await?;
        Self::await_protocol_response(PROTOCOL_NAME_FINALITY, rx).await
    }

    async fn send_finality_response_message(
        &self,
        response: ResponseHandle<FinalityAck>,
        message: ResponseMessage<FinalityAck>,
    ) -> Result<(), NetworkError> {
        self.enqueue_data_action(NetworkDataAction::FinalityResponse {
            channel: response.into_inner(),
            message,
        })
        .await
    }

    /// Send a finality ACK response for an inbound request.
    pub async fn send_finality_ack(
        &self,
        response: ResponseHandle<FinalityAck>,
        operation_id: Uuid,
        ack: FinalityAck,
    ) -> Result<(), NetworkError> {
        self.send_finality_response_message(response, ResponseMessage::ack(operation_id, ack))
            .await
    }

    /// Send a finality NACK response for an inbound request.
    pub async fn send_finality_nack(
        &self,
        response: ResponseHandle<FinalityAck>,
        operation_id: Uuid,
        error_message: impl Into<String>,
    ) -> Result<(), NetworkError> {
        self.send_finality_response_message(
            response,
            ResponseMessage::nack(operation_id, error_message),
        )
        .await
    }

    /// Send a batch get request and await the response.
    #[instrument(
        name = "network_batch_get",
        skip(self, request_data),
        fields(peer_id = %peer, operation_id = %operation_id)
    )]
    pub async fn send_batch_get_request(
        &self,
        peer: PeerId,
        operation_id: Uuid,
        request_data: BatchGetRequestData,
    ) -> Result<BatchGetResponseData, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_data_action(NetworkDataAction::BatchGetRequest {
            peer,
            operation_id,
            request_data,
            response_tx: tx,
        })
        .await?;
        Self::await_protocol_response(PROTOCOL_NAME_BATCH_GET, rx).await
    }

    async fn send_batch_get_response_message(
        &self,
        response: ResponseHandle<BatchGetAck>,
        message: ResponseMessage<BatchGetAck>,
    ) -> Result<(), NetworkError> {
        self.enqueue_data_action(NetworkDataAction::BatchGetResponse {
            channel: response.into_inner(),
            message,
        })
        .await
    }

    /// Send a batch-get ACK response for an inbound request.
    pub async fn send_batch_get_ack(
        &self,
        response: ResponseHandle<BatchGetAck>,
        operation_id: Uuid,
        ack: BatchGetAck,
    ) -> Result<(), NetworkError> {
        self.send_batch_get_response_message(response, ResponseMessage::ack(operation_id, ack))
            .await
    }

    /// Send a batch-get NACK response for an inbound request.
    pub async fn send_batch_get_nack(
        &self,
        response: ResponseHandle<BatchGetAck>,
        operation_id: Uuid,
        error_message: impl Into<String>,
    ) -> Result<(), NetworkError> {
        self.send_batch_get_response_message(
            response,
            ResponseMessage::nack(operation_id, error_message),
        )
        .await
    }
}
