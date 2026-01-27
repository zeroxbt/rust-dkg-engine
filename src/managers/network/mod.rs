pub(crate) mod error;
mod key_manager;
pub(crate) mod message;
pub(crate) mod messages;
mod pending_requests;
pub(crate) mod protocols;

use std::time::Duration;

use error::NetworkError;
pub(crate) use key_manager::KeyManager;
pub(crate) use libp2p::request_response::ProtocolSupport;
// Re-export libp2p types and identity for application use
pub(crate) use libp2p::{
    Multiaddr, PeerId, StreamProtocol, Swarm, identify, identity,
    kad::{self, BucketInserts, Config as KademliaConfig, Mode, store::MemoryStore},
    request_response,
    swarm::{NetworkBehaviour, SwarmEvent},
};
// Internal libp2p imports for NetworkManager implementation
use libp2p::{SwarmBuilder, noise, tcp};
// Re-export message types
pub(crate) use message::{RequestMessage, ResponseMessage};
pub(crate) use pending_requests::{PendingRequests, RequestError};
pub(crate) use protocols::{NetworkProtocols, NetworkProtocolsEvent};
use serde::Deserialize;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::info;
use uuid::Uuid;

use self::{
    message::{RequestMessageHeader, RequestMessageType},
    messages::{
        BatchGetRequestData, BatchGetResponseData, FinalityRequestData, FinalityResponseData,
        GetRequestData, GetResponseData, StoreRequestData, StoreResponseData,
    },
};

/// Network action commands sent to the swarm event loop.
enum NetworkAction {
    /// Discover peers via Kademlia DHT lookup (get_closest_peers).
    /// This queries the network for nodes closest to the given peer IDs,
    /// which populates the routing table with their addresses.
    FindPeers(Vec<PeerId>),
    /// Directly dial a peer (requires addresses to be known in Kademlia).
    DialPeer(PeerId),
    /// Get the list of currently connected peers.
    GetConnectedPeers(oneshot::Sender<Vec<PeerId>>),
    /// Get known addresses for a peer from Kademlia routing table.
    GetPeerAddresses {
        peer_id: PeerId,
        response_tx: oneshot::Sender<Vec<Multiaddr>>,
    },
    AddKadAddresses {
        peer_id: PeerId,
        listen_addrs: Vec<Multiaddr>,
    },
    // Protocol-specific request actions
    SendStoreRequest {
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        operation_id: Uuid,
        request_data: StoreRequestData,
        response_tx: oneshot::Sender<oneshot::Receiver<Result<StoreResponseData, RequestError>>>,
    },
    SendGetRequest {
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        operation_id: Uuid,
        request_data: GetRequestData,
        response_tx: oneshot::Sender<oneshot::Receiver<Result<GetResponseData, RequestError>>>,
    },
    SendFinalityRequest {
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        operation_id: Uuid,
        request_data: FinalityRequestData,
        response_tx:
            oneshot::Sender<oneshot::Receiver<Result<FinalityResponseData, RequestError>>>,
    },
    SendBatchGetRequest {
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        operation_id: Uuid,
        request_data: BatchGetRequestData,
        response_tx:
            oneshot::Sender<oneshot::Receiver<Result<BatchGetResponseData, RequestError>>>,
    },
    // Protocol-specific response actions
    SendStoreResponse {
        channel: request_response::ResponseChannel<ResponseMessage<StoreResponseData>>,
        message: ResponseMessage<StoreResponseData>,
    },
    SendGetResponse {
        channel: request_response::ResponseChannel<ResponseMessage<GetResponseData>>,
        message: ResponseMessage<GetResponseData>,
    },
    SendFinalityResponse {
        channel: request_response::ResponseChannel<ResponseMessage<FinalityResponseData>>,
        message: ResponseMessage<FinalityResponseData>,
    },
    SendBatchGetResponse {
        channel: request_response::ResponseChannel<ResponseMessage<BatchGetResponseData>>,
        message: ResponseMessage<BatchGetResponseData>,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct NetworkManagerConfig {
    pub port: u32,
    pub bootstrap: Vec<String>,
    /// External IP address to announce to peers (for NAT traversal).
    /// If set, the node will advertise this address so peers behind NAT can be reached.
    /// Must be a valid public IPv4 address.
    #[serde(default)]
    pub external_ip: Option<String>,
    /// How long to keep idle connections open (in seconds).
    /// Default is 300 (5 minutes).
    #[serde(default = "default_idle_connection_timeout")]
    pub idle_connection_timeout_secs: u64,
}

fn default_idle_connection_timeout() -> u64 {
    300
}

impl NetworkManagerConfig {
    /// Get idle connection timeout as Duration
    pub(crate) fn idle_connection_timeout(&self) -> Duration {
        Duration::from_secs(self.idle_connection_timeout_secs)
    }
}

/// Hierarchical network behaviour that combines base protocols with app-specific protocols
///
/// NetworkManager provides the base protocols (kad, identify) and the application
/// provides its custom protocols via the NetworkProtocols struct.
#[derive(NetworkBehaviour)]
pub(crate) struct CompositeBehaviour {
    pub kad: kad::Behaviour<MemoryStore>,
    pub identify: identify::Behaviour,
    pub protocols: NetworkProtocols,
}

/// NetworkManager handles all network communication for the node.
///
/// It owns the protocol behaviours and pending request tracking, which allows
/// for atomic registration of pending requests before sending to avoid race conditions.
pub(crate) struct NetworkManager {
    config: NetworkManagerConfig,
    swarm: Mutex<Swarm<CompositeBehaviour>>,
    action_tx: mpsc::Sender<NetworkAction>,
    action_rx: Mutex<Option<mpsc::Receiver<NetworkAction>>>,
    peer_id: PeerId,
    // Per-protocol pending request tracking
    pending_store: PendingRequests<StoreResponseData>,
    pending_get: PendingRequests<GetResponseData>,
    pending_finality: PendingRequests<FinalityResponseData>,
    pending_batch_get: PendingRequests<BatchGetResponseData>,
}

impl NetworkManager {
    /// Creates a new NetworkManager instance.
    ///
    /// # Arguments
    /// * `config` - Network configuration (port, bootstrap nodes)
    /// * `key` - Pre-loaded libp2p identity keypair
    ///
    /// # Errors
    ///
    /// Returns `NetworkError` if:
    /// - Bootstrap node parsing fails
    /// - Transport creation fails
    /// - Swarm building fails
    pub(crate) async fn connect(
        config: &NetworkManagerConfig,
        key: identity::Keypair,
    ) -> Result<Self, NetworkError> {
        let public_key = key.public();
        let local_peer_id = PeerId::from(&public_key);

        info!("Network ID is {}", local_peer_id.to_base58());

        // Create base protocols
        // 1. Kademlia DHT
        let mut kad_config = KademliaConfig::default();
        kad_config.set_kbucket_inserts(BucketInserts::OnConnected);
        let memory_store = MemoryStore::new(local_peer_id);
        let mut kad = kad::Behaviour::with_config(local_peer_id, memory_store, kad_config);

        kad.set_mode(Some(Mode::Server));

        // Add bootstrap nodes to kad
        for bootstrap in &config.bootstrap {
            let parts: Vec<&str> = bootstrap.split("/p2p/").collect();

            if parts.len() != 2 {
                return Err(NetworkError::InvalidBootstrapNode {
                    expected: "/ip4/.../p2p/...".to_string(),
                    received: bootstrap.to_string(),
                });
            }

            let bootstrap_peer_id = parts[1].parse().map_err(|e| NetworkError::InvalidPeerId {
                parsed: parts[1].to_string(),
                source: e,
            })?;

            let bootstrap_address =
                parts[0]
                    .parse()
                    .map_err(|e| NetworkError::InvalidMultiaddr {
                        parsed: parts[0].to_string(),
                        source: e,
                    })?;

            kad.add_address(&bootstrap_peer_id, bootstrap_address);
        }

        // 2. Identify protocol
        let identify = identify::Behaviour::new(identify::Config::new(
            "/ipfs/id/1.0.0".to_string(),
            public_key.clone(),
        ));

        // 3. Application protocols
        let protocols = NetworkProtocols::new();

        let custom_behaviour = CompositeBehaviour {
            kad,
            identify,
            protocols,
        };

        // Build the swarm with configurable idle connection timeout.
        // Default libp2p timeout is 10 seconds, which causes connections to drop quickly.
        // We use a configurable timeout (default 5 minutes) to maintain shard table connections.
        let idle_timeout = config.idle_connection_timeout();
        let mut swarm = SwarmBuilder::with_existing_identity(key.clone())
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                libp2p_mplex::Config::default,
            )
            .map_err(NetworkError::TransportCreation)?
            .with_behaviour(|_| custom_behaviour)?
            .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(idle_timeout))
            .build();

        // Add external address for NAT traversal if configured.
        // This allows peers behind NAT to be reachable by advertising their public IP.
        if let Some(ref external_ip) = config.external_ip {
            // Validate that it's a valid public IPv4 address
            match external_ip.parse::<std::net::Ipv4Addr>() {
                Ok(ip) if !ip.is_private() && !ip.is_loopback() && !ip.is_link_local() => {
                    let external_addr: Multiaddr =
                        format!("/ip4/{}/tcp/{}", external_ip, config.port)
                            .parse()
                            .map_err(|e| NetworkError::InvalidMultiaddr {
                                parsed: format!("/ip4/{}/tcp/{}", external_ip, config.port),
                                source: e,
                            })?;
                    swarm.add_external_address(external_addr.clone());
                    info!(
                        "Added external address for NAT traversal: {}",
                        external_addr
                    );
                }
                Ok(_) => {
                    tracing::warn!(
                        external_ip,
                        "external_ip must be a public IPv4 address, ignoring"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        external_ip,
                        error = %e,
                        "invalid external_ip format, must be a valid IPv4 address"
                    );
                }
            }
        }

        let (action_tx, action_rx) = mpsc::channel(1024);

        Ok(Self {
            config: config.to_owned(),
            swarm: tokio::sync::Mutex::new(swarm),
            action_tx,
            action_rx: Mutex::new(Some(action_rx)),
            peer_id: local_peer_id,
            pending_store: PendingRequests::new(),
            pending_get: PendingRequests::new(),
            pending_finality: PendingRequests::new(),
            pending_batch_get: PendingRequests::new(),
        })
    }

    /// Starts listening on the configured port
    ///
    /// # Errors
    ///
    /// Returns `NetworkError` if:
    /// - Multiaddr parsing fails
    /// - Listener initialization fails
    pub(crate) async fn start_listening(&self) -> Result<(), NetworkError> {
        let listen_addr = format!("/ip4/0.0.0.0/tcp/{}", self.config.port);
        let listen_addr: libp2p::Multiaddr =
            listen_addr
                .parse()
                .map_err(|e| NetworkError::InvalidMultiaddr {
                    parsed: listen_addr,
                    source: e,
                })?;

        let mut swarm = self.swarm.lock().await;

        swarm
            .listen_on(listen_addr.clone())
            .map_err(|e| NetworkError::ListenerFailed {
                address: listen_addr.to_string(),
                source: Box::new(e),
            })?;

        Ok(())
    }

    /// Returns the peer ID of this node
    pub(crate) fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    /// Returns a reference to pending store requests for event handling.
    pub(crate) fn pending_store(&self) -> &PendingRequests<StoreResponseData> {
        &self.pending_store
    }

    /// Returns a reference to pending get requests for event handling.
    pub(crate) fn pending_get(&self) -> &PendingRequests<GetResponseData> {
        &self.pending_get
    }

    /// Returns a reference to pending finality requests for event handling.
    pub(crate) fn pending_finality(&self) -> &PendingRequests<FinalityResponseData> {
        &self.pending_finality
    }

    /// Returns a reference to pending batch get requests for event handling.
    pub(crate) fn pending_batch_get(&self) -> &PendingRequests<BatchGetResponseData> {
        &self.pending_batch_get
    }

    /// Runs the network event loop, processing swarm events and actions
    ///
    /// This method runs the event loop using the internal swarm and action channel.
    /// It receives actions from the internal action channel and sends events to the event_tx
    /// channel.
    ///
    /// # Parameters
    /// - `event_tx`: Channel sender for outgoing swarm events
    pub(crate) async fn run(
        &self,
        event_tx: mpsc::Sender<SwarmEvent<<CompositeBehaviour as NetworkBehaviour>::ToSwarm>>,
    ) {
        use libp2p::futures::StreamExt;

        let Some(mut action_rx) = self.action_rx.lock().await.take() else {
            tracing::error!("Action receiver already taken, shutting down network loop");
            return;
        };

        let mut swarm = self.swarm.lock().await;

        loop {
            tokio::select! {
                event = swarm.select_next_some() => {
                    // Send event to application layer
                    if event_tx.send(event).await.is_err() {
                        tracing::error!("Event channel closed, shutting down network loop");
                        break;
                    }
                }
                action = action_rx.recv() => {
                    match action {
                        Some(action) => self.handle_action(&mut swarm, action),
                        None => {
                            tracing::error!("Action channel closed, shutting down network loop");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Handle a network action inside the swarm event loop.
    fn handle_action(&self, swarm: &mut Swarm<CompositeBehaviour>, action: NetworkAction) {
        match action {
            NetworkAction::FindPeers(peers) => {
                for peer in peers {
                    swarm.behaviour_mut().kad.get_closest_peers(peer);
                }
            }
            NetworkAction::DialPeer(peer) => {
                if swarm.is_connected(&peer) {
                    tracing::trace!(%peer, "already connected to peer, skipping dial");
                } else if let Err(e) = swarm.dial(peer) {
                    tracing::debug!(%peer, error = %e, "failed to dial peer");
                }
            }
            NetworkAction::GetConnectedPeers(response_tx) => {
                let peers: Vec<PeerId> = swarm.connected_peers().copied().collect();
                let _ = response_tx.send(peers);
            }
            NetworkAction::GetPeerAddresses {
                peer_id,
                response_tx,
            } => {
                // Get addresses from Kademlia routing table using the specific
                // k-bucket for this peer (more efficient than iterating all buckets)
                let addresses = swarm
                    .behaviour_mut()
                    .kad
                    .kbucket(peer_id)
                    .and_then(|bucket| {
                        bucket
                            .iter()
                            .find(|entry| *entry.node.key.preimage() == peer_id)
                            .map(|entry| entry.node.value.iter().cloned().collect())
                    })
                    .unwrap_or_default();
                let _ = response_tx.send(addresses);
            }
            NetworkAction::AddKadAddresses {
                peer_id,
                listen_addrs,
            } => {
                for address in listen_addrs {
                    swarm.behaviour_mut().kad.add_address(&peer_id, address);
                }
            }
            // Store protocol
            NetworkAction::SendStoreRequest {
                peer,
                addresses,
                operation_id,
                request_data,
                response_tx,
            } => {
                // Wrap request data in protocol message
                let message = RequestMessage {
                    header: RequestMessageHeader::new(operation_id, RequestMessageType::ProtocolRequest),
                    data: request_data,
                };
                // Atomically register pending request BEFORE sending to avoid race condition
                let request_id = swarm
                    .behaviour_mut()
                    .protocols
                    .store
                    .send_request_with_addresses(&peer, message, addresses);
                let receiver = self.pending_store.insert(request_id);
                let _ = response_tx.send(receiver);
            }
            NetworkAction::SendStoreResponse { channel, message } => {
                let _ = swarm
                    .behaviour_mut()
                    .protocols
                    .store
                    .send_response(channel, message);
            }
            // Get protocol
            NetworkAction::SendGetRequest {
                peer,
                addresses,
                operation_id,
                request_data,
                response_tx,
            } => {
                let message = RequestMessage {
                    header: RequestMessageHeader::new(operation_id, RequestMessageType::ProtocolRequest),
                    data: request_data,
                };
                let request_id = swarm
                    .behaviour_mut()
                    .protocols
                    .get
                    .send_request_with_addresses(&peer, message, addresses);
                let receiver = self.pending_get.insert(request_id);
                let _ = response_tx.send(receiver);
            }
            NetworkAction::SendGetResponse { channel, message } => {
                let _ = swarm
                    .behaviour_mut()
                    .protocols
                    .get
                    .send_response(channel, message);
            }
            // Finality protocol
            NetworkAction::SendFinalityRequest {
                peer,
                addresses,
                operation_id,
                request_data,
                response_tx,
            } => {
                let message = RequestMessage {
                    header: RequestMessageHeader::new(operation_id, RequestMessageType::ProtocolRequest),
                    data: request_data,
                };
                let request_id = swarm
                    .behaviour_mut()
                    .protocols
                    .finality
                    .send_request_with_addresses(&peer, message, addresses);
                let receiver = self.pending_finality.insert(request_id);
                let _ = response_tx.send(receiver);
            }
            NetworkAction::SendFinalityResponse { channel, message } => {
                let _ = swarm
                    .behaviour_mut()
                    .protocols
                    .finality
                    .send_response(channel, message);
            }
            // BatchGet protocol
            NetworkAction::SendBatchGetRequest {
                peer,
                addresses,
                operation_id,
                request_data,
                response_tx,
            } => {
                let message = RequestMessage {
                    header: RequestMessageHeader::new(operation_id, RequestMessageType::ProtocolRequest),
                    data: request_data,
                };
                let request_id = swarm
                    .behaviour_mut()
                    .protocols
                    .batch_get
                    .send_request_with_addresses(&peer, message, addresses);
                let receiver = self.pending_batch_get.insert(request_id);
                let _ = response_tx.send(receiver);
            }
            NetworkAction::SendBatchGetResponse { channel, message } => {
                let _ = swarm
                    .behaviour_mut()
                    .protocols
                    .batch_get
                    .send_response(channel, message);
            }
        }
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

    /// Directly dial a peer to establish a connection.
    /// The peer's addresses must already be known (e.g., from a previous DHT lookup).
    pub(crate) async fn dial_peer(&self, peer: PeerId) -> Result<(), NetworkError> {
        self.enqueue_action(NetworkAction::DialPeer(peer)).await
    }

    /// Get the list of currently connected peers.
    pub(crate) async fn connected_peers(&self) -> Result<Vec<PeerId>, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_action(NetworkAction::GetConnectedPeers(tx))
            .await?;
        rx.await.map_err(|_| NetworkError::RequestIdChannelClosed)
    }

    /// Get known addresses for a peer from the Kademlia routing table.
    /// Returns an empty vector if the peer is not found.
    pub(crate) async fn get_peer_addresses(
        &self,
        peer_id: PeerId,
    ) -> Result<Vec<Multiaddr>, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_action(NetworkAction::GetPeerAddresses {
            peer_id,
            response_tx: tx,
        })
        .await?;
        rx.await.map_err(|_| NetworkError::RequestIdChannelClosed)
    }

    /// Enqueue Kademlia address updates for a peer.
    pub(crate) async fn add_kad_addresses(
        &self,
        peer_id: PeerId,
        listen_addrs: Vec<Multiaddr>,
    ) -> Result<(), NetworkError> {
        self.enqueue_action(NetworkAction::AddKadAddresses {
            peer_id,
            listen_addrs,
        })
        .await
    }

    // Protocol-specific send methods that send a request and await the response.
    // These methods atomically register the pending request inside the network loop
    // BEFORE the request is sent, preventing race conditions with DialFailure/Timeout events.
    // The message wrapping (RequestMessage with header) is handled internally.

    /// Send a store request and await the response.
    pub(crate) async fn send_store_request(
        &self,
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        operation_id: Uuid,
        request_data: StoreRequestData,
    ) -> Result<StoreResponseData, RequestError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_action(NetworkAction::SendStoreRequest {
            peer,
            addresses,
            operation_id,
            request_data,
            response_tx: tx,
        })
        .await
        .map_err(|e| RequestError::ConnectionFailed(e.to_string()))?;
        let response_rx = rx
            .await
            .map_err(|_| RequestError::ConnectionFailed("Action channel closed".to_string()))?;
        response_rx
            .await
            .map_err(|_| RequestError::ChannelClosed)?
    }

    /// Send a store response.
    pub(crate) async fn send_store_response(
        &self,
        channel: request_response::ResponseChannel<ResponseMessage<StoreResponseData>>,
        message: ResponseMessage<StoreResponseData>,
    ) -> Result<(), NetworkError> {
        self.enqueue_action(NetworkAction::SendStoreResponse { channel, message })
            .await
    }

    /// Send a get request and await the response.
    pub(crate) async fn send_get_request(
        &self,
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        operation_id: Uuid,
        request_data: GetRequestData,
    ) -> Result<GetResponseData, RequestError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_action(NetworkAction::SendGetRequest {
            peer,
            addresses,
            operation_id,
            request_data,
            response_tx: tx,
        })
        .await
        .map_err(|e| RequestError::ConnectionFailed(e.to_string()))?;
        let response_rx = rx
            .await
            .map_err(|_| RequestError::ConnectionFailed("Action channel closed".to_string()))?;
        response_rx
            .await
            .map_err(|_| RequestError::ChannelClosed)?
    }

    /// Send a get response.
    pub(crate) async fn send_get_response(
        &self,
        channel: request_response::ResponseChannel<ResponseMessage<GetResponseData>>,
        message: ResponseMessage<GetResponseData>,
    ) -> Result<(), NetworkError> {
        self.enqueue_action(NetworkAction::SendGetResponse { channel, message })
            .await
    }

    /// Send a finality request and await the response.
    pub(crate) async fn send_finality_request(
        &self,
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        operation_id: Uuid,
        request_data: FinalityRequestData,
    ) -> Result<FinalityResponseData, RequestError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_action(NetworkAction::SendFinalityRequest {
            peer,
            addresses,
            operation_id,
            request_data,
            response_tx: tx,
        })
        .await
        .map_err(|e| RequestError::ConnectionFailed(e.to_string()))?;
        let response_rx = rx
            .await
            .map_err(|_| RequestError::ConnectionFailed("Action channel closed".to_string()))?;
        response_rx
            .await
            .map_err(|_| RequestError::ChannelClosed)?
    }

    /// Send a finality response.
    pub(crate) async fn send_finality_response(
        &self,
        channel: request_response::ResponseChannel<ResponseMessage<FinalityResponseData>>,
        message: ResponseMessage<FinalityResponseData>,
    ) -> Result<(), NetworkError> {
        self.enqueue_action(NetworkAction::SendFinalityResponse { channel, message })
            .await
    }

    /// Send a batch get request and await the response.
    pub(crate) async fn send_batch_get_request(
        &self,
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        operation_id: Uuid,
        request_data: BatchGetRequestData,
    ) -> Result<BatchGetResponseData, RequestError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_action(NetworkAction::SendBatchGetRequest {
            peer,
            addresses,
            operation_id,
            request_data,
            response_tx: tx,
        })
        .await
        .map_err(|e| RequestError::ConnectionFailed(e.to_string()))?;
        let response_rx = rx
            .await
            .map_err(|_| RequestError::ConnectionFailed("Action channel closed".to_string()))?;
        response_rx
            .await
            .map_err(|_| RequestError::ChannelClosed)?
    }

    /// Send a batch get response.
    pub(crate) async fn send_batch_get_response(
        &self,
        channel: request_response::ResponseChannel<ResponseMessage<BatchGetResponseData>>,
        message: ResponseMessage<BatchGetResponseData>,
    ) -> Result<(), NetworkError> {
        self.enqueue_action(NetworkAction::SendBatchGetResponse { channel, message })
            .await
    }
}
