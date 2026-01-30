pub(crate) mod error;
mod handler;
mod key_manager;
pub(crate) mod message;
pub(crate) mod messages;
mod pending_requests;
pub(crate) mod protocols;

use std::time::Duration;

pub(crate) use error::NetworkError;
pub(crate) use handler::NetworkEventHandler;
pub(crate) use key_manager::KeyManager;
pub(crate) use libp2p::request_response::ProtocolSupport;
// Re-export libp2p types and identity for application use
pub(crate) use libp2p::{
    Multiaddr, PeerId, StreamProtocol, Swarm, identify, identity,
    kad::{self, BucketInserts, Config as KademliaConfig, Mode, store::MemoryStore},
    multiaddr::Protocol,
    request_response,
    swarm::{NetworkBehaviour, SwarmEvent},
};
// Internal libp2p imports for NetworkManager implementation
use libp2p::{SwarmBuilder, noise, tcp};
// Re-export message types
pub(crate) use message::{RequestMessage, ResponseMessage};
pub(crate) use pending_requests::PendingRequests;
pub(crate) use protocols::{JsCompatCodec, ProtocolTimeouts};
use serde::Deserialize;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::{info, instrument};
use uuid::Uuid;

use self::{
    message::{RequestMessageHeader, RequestMessageType},
    messages::{
        BatchGetAck, BatchGetRequestData, BatchGetResponseData, FinalityAck, FinalityRequestData,
        FinalityResponseData, GetAck, GetRequestData, GetResponseData, StoreAck, StoreRequestData,
        StoreResponseData,
    },
};

/// Macro to send a protocol request to a peer.
/// Reduces boilerplate for the identical pattern across all 4 protocols.
///
/// The response_tx is stored directly in PendingRequests, eliminating the need
/// for nested oneshot channels. When the response arrives, it's sent directly
/// through response_tx.
macro_rules! send_protocol_request {
    ($swarm:expr, $self:expr, $peer:expr, $addresses:expr, $operation_id:expr,
     $request_data:expr, $response_tx:expr, $protocol:ident, $pending:ident) => {{
        let message = RequestMessage {
            header: RequestMessageHeader::new($operation_id, RequestMessageType::ProtocolRequest),
            data: $request_data,
        };
        // Send request and get request_id
        let request_id = $swarm
            .behaviour_mut()
            .$protocol
            .send_request(&$peer, message);
        // Store the response channel directly - it will be used when the response arrives
        $self.$pending.insert_sender(request_id, $response_tx);
    }};
}

/// Macro to send a protocol response to a peer.
macro_rules! send_protocol_response {
    ($swarm:expr, $channel:expr, $message:expr, $protocol:ident) => {{
        let _ = $swarm
            .behaviour_mut()
            .$protocol
            .send_response($channel, $message);
    }};
}

/// Macro to handle a protocol event - processes responses internally and dispatches
/// inbound requests to the handler.
macro_rules! handle_protocol_event {
    ($event:expr, $pending:expr, $protocol_name:literal, $handler:expr, $handler_method:ident) => {
        match $event {
            request_response::Event::Message { message, peer, .. } => match message {
                request_response::Message::Response {
                    response,
                    request_id,
                } => {
                    $pending.complete_success(request_id, response.data);
                }
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    $handler.$handler_method(request, channel, peer).await;
                }
            },
            request_response::Event::OutboundFailure {
                request_id,
                error,
                peer,
                ..
            } => {
                tracing::warn!(%peer, ?request_id, ?error, concat!($protocol_name, " request failed"));
                $pending.complete_failure(request_id, NetworkError::from(&error));
            }
            request_response::Event::InboundFailure {
                peer,
                request_id,
                error,
                ..
            } => {
                tracing::warn!(
                    %peer,
                    ?request_id,
                    ?error,
                    concat!("Failed to respond to ", $protocol_name, " request")
                );
            }
            request_response::Event::ResponseSent {
                peer, request_id, ..
            } => {
                tracing::trace!(%peer, ?request_id, concat!($protocol_name, " response sent"));
            }
        }
    };
}

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
    // The response_tx is stored in PendingRequests and used to deliver the response directly
    SendStoreRequest {
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        operation_id: Uuid,
        request_data: StoreRequestData,
        response_tx: oneshot::Sender<Result<StoreResponseData, NetworkError>>,
    },
    SendGetRequest {
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        operation_id: Uuid,
        request_data: GetRequestData,
        response_tx: oneshot::Sender<Result<GetResponseData, NetworkError>>,
    },
    SendFinalityRequest {
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        operation_id: Uuid,
        request_data: FinalityRequestData,
        response_tx: oneshot::Sender<Result<FinalityResponseData, NetworkError>>,
    },
    SendBatchGetRequest {
        peer: PeerId,
        addresses: Vec<Multiaddr>,
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

use crate::services::PeerRateLimiterConfig;

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
    /// Per-peer rate limiting configuration for inbound requests.
    #[serde(default)]
    pub rate_limiter: PeerRateLimiterConfig,
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

/// Flattened network behaviour combining all protocols used by the node.
///
/// This includes both infrastructure protocols (kad, identify) and
/// application-specific request-response protocols (store, get, finality, batch_get).
#[derive(NetworkBehaviour)]
pub(crate) struct NodeBehaviour {
    pub kad: kad::Behaviour<MemoryStore>,
    pub identify: identify::Behaviour,
    // Application protocols using JsCompatCodec for JS node interoperability
    pub store: request_response::Behaviour<
        JsCompatCodec<RequestMessage<StoreRequestData>, ResponseMessage<StoreAck>>,
    >,
    pub get: request_response::Behaviour<
        JsCompatCodec<RequestMessage<GetRequestData>, ResponseMessage<GetAck>>,
    >,
    pub finality: request_response::Behaviour<
        JsCompatCodec<RequestMessage<FinalityRequestData>, ResponseMessage<FinalityAck>>,
    >,
    pub batch_get: request_response::Behaviour<
        JsCompatCodec<RequestMessage<BatchGetRequestData>, ResponseMessage<BatchGetAck>>,
    >,
}

/// NetworkManager handles all network communication for the node.
///
/// It owns the protocol behaviours and pending request tracking, which allows
/// for atomic registration of pending requests before sending to avoid race conditions.
pub(crate) struct NetworkManager {
    config: NetworkManagerConfig,
    swarm: Mutex<Swarm<NodeBehaviour>>,
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
            // Parse as a full multiaddr first
            let full_addr: Multiaddr =
                bootstrap
                    .parse()
                    .map_err(|e| NetworkError::InvalidMultiaddr {
                        parsed: bootstrap.clone(),
                        source: e,
                    })?;

            // Extract the peer ID from the /p2p/ component
            let peer_id = full_addr
                .iter()
                .find_map(|proto| {
                    if let Protocol::P2p(peer_id) = proto {
                        Some(peer_id)
                    } else {
                        None
                    }
                })
                .ok_or_else(|| NetworkError::InvalidBootstrapNode {
                    expected: "multiaddr with /p2p/<peer_id> component".to_string(),
                    received: bootstrap.clone(),
                })?;

            // Build the address without the /p2p/ component for kad
            let addr_without_peer: Multiaddr = full_addr
                .iter()
                .filter(|proto| !matches!(proto, Protocol::P2p(_)))
                .collect();

            kad.add_address(&peer_id, addr_without_peer);
        }

        // 2. Identify protocol
        let identify = identify::Behaviour::new(identify::Config::new(
            "/ipfs/id/1.0.0".to_string(),
            public_key.clone(),
        ));

        // 3. Application protocols (store, get, finality, batch_get)
        let store = request_response::Behaviour::with_codec(
            JsCompatCodec::new(),
            [(StreamProtocol::new("/store/1.0.0"), ProtocolSupport::Full)],
            request_response::Config::default().with_request_timeout(ProtocolTimeouts::STORE),
        );
        let get = request_response::Behaviour::with_codec(
            JsCompatCodec::new(),
            [(StreamProtocol::new("/get/1.0.0"), ProtocolSupport::Full)],
            request_response::Config::default().with_request_timeout(ProtocolTimeouts::GET),
        );
        let finality = request_response::Behaviour::with_codec(
            JsCompatCodec::new(),
            [(
                StreamProtocol::new("/finality/1.0.0"),
                ProtocolSupport::Full,
            )],
            request_response::Config::default().with_request_timeout(ProtocolTimeouts::FINALITY),
        );
        let batch_get = request_response::Behaviour::with_codec(
            JsCompatCodec::new(),
            [(
                StreamProtocol::new("/batch-get/1.0.0"),
                ProtocolSupport::Full,
            )],
            request_response::Config::default().with_request_timeout(ProtocolTimeouts::BATCH_GET),
        );

        let behaviour = NodeBehaviour {
            kad,
            identify,
            store,
            get,
            finality,
            batch_get,
        };

        // Build the swarm with configurable idle connection timeout.
        // Default libp2p timeout is 10 seconds, which causes connections to drop quickly.
        // We use a configurable timeout (default 5 minutes) to maintain shard table connections.
        let idle_timeout = config.idle_connection_timeout();
        // Use yamux as the preferred multiplexer (better flow control, no head-of-line blocking)
        // with mplex as fallback for compatibility with older JS libp2p nodes.
        // Protocol negotiation selects the best common option automatically.
        let mut swarm = SwarmBuilder::with_existing_identity(key.clone())
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                (
                    libp2p::yamux::Config::default,
                    libp2p_mplex::Config::default,
                ),
            )
            .map_err(NetworkError::TransportCreation)?
            .with_behaviour(|_| behaviour)?
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

        let (action_tx, action_rx) = mpsc::channel(128);

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

    /// Runs the network event loop, processing swarm events and actions.
    ///
    /// This method runs the event loop using the internal swarm and action channel.
    /// Response events (success/failure for outbound requests) are handled internally
    /// via PendingRequests. Inbound requests and infrastructure events are dispatched
    /// to the provided handler.
    ///
    /// # Parameters
    /// - `handler`: Handler for network events that require application-level processing
    pub(crate) async fn run<H: NetworkEventHandler>(&self, handler: &H) {
        use libp2p::futures::StreamExt;

        let Some(mut action_rx) = self.action_rx.lock().await.take() else {
            tracing::error!("Action receiver already taken, shutting down network loop");
            return;
        };

        let mut swarm = self.swarm.lock().await;

        loop {
            tokio::select! {
                event = swarm.select_next_some() => {
                    // Release swarm lock before calling handler to avoid deadlocks
                    drop(swarm);
                    self.handle_swarm_event(event, handler).await;
                    swarm = self.swarm.lock().await;
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

    /// Process a swarm event, handling responses internally and dispatching
    /// inbound requests to the handler.
    async fn handle_swarm_event<H: NetworkEventHandler>(
        &self,
        event: SwarmEvent<<NodeBehaviour as NetworkBehaviour>::ToSwarm>,
        handler: &H,
    ) {
        match event {
            SwarmEvent::Behaviour(behaviour_event) => match behaviour_event {
                // ─────────────────────────────────────────────────────────────
                // Protocol events - handle responses internally, dispatch requests
                // ─────────────────────────────────────────────────────────────
                NodeBehaviourEvent::Store(inner) => {
                    handle_protocol_event!(
                        inner,
                        self.pending_store,
                        "Store",
                        handler,
                        on_store_request
                    );
                }
                NodeBehaviourEvent::Get(inner) => {
                    handle_protocol_event!(inner, self.pending_get, "Get", handler, on_get_request);
                }
                NodeBehaviourEvent::Finality(inner) => {
                    handle_protocol_event!(
                        inner,
                        self.pending_finality,
                        "Finality",
                        handler,
                        on_finality_request
                    );
                }
                NodeBehaviourEvent::BatchGet(inner) => {
                    handle_protocol_event!(
                        inner,
                        self.pending_batch_get,
                        "BatchGet",
                        handler,
                        on_batch_get_request
                    );
                }

                // ─────────────────────────────────────────────────────────────
                // Identify protocol
                // ─────────────────────────────────────────────────────────────
                NodeBehaviourEvent::Identify(identify::Event::Received {
                    peer_id, info, ..
                }) => {
                    handler
                        .on_identify_received(peer_id, info.listen_addrs)
                        .await;
                }

                // ─────────────────────────────────────────────────────────────
                // Kademlia protocol
                // ─────────────────────────────────────────────────────────────
                NodeBehaviourEvent::Kad(kad::Event::OutboundQueryProgressed {
                    result: kad::QueryResult::GetClosestPeers(result),
                    ..
                }) => match result {
                    Ok(kad::GetClosestPeersOk { key, peers }) => {
                        if let Ok(target) = PeerId::from_bytes(&key) {
                            if peers.iter().any(|p| p.peer_id == target) {
                                handler.on_kad_peer_found(target).await;
                            } else {
                                handler.on_kad_peer_not_found(target).await;
                            }
                        }
                    }
                    Err(kad::GetClosestPeersError::Timeout { key, peers }) => {
                        if let Ok(target) = PeerId::from_bytes(&key) {
                            if peers.iter().any(|p| p.peer_id == target) {
                                handler.on_kad_peer_found(target).await;
                            } else {
                                handler.on_kad_peer_not_found(target).await;
                            }
                        }
                    }
                },

                _ => {}
            },

            SwarmEvent::NewListenAddr { address, .. } => {
                handler.on_new_listen_addr(address);
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                handler.on_connection_established(peer_id).await;
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                handler.on_connection_closed(peer_id).await;
            }
            _ => {}
        }
    }

    /// Handle a network action inside the swarm event loop.
    fn handle_action(&self, swarm: &mut Swarm<NodeBehaviour>, action: NetworkAction) {
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
            // Protocol request/response actions - using macros to reduce boilerplate
            NetworkAction::SendStoreRequest {
                peer,
                addresses,
                operation_id,
                request_data,
                response_tx,
            } => send_protocol_request!(
                swarm,
                self,
                peer,
                addresses,
                operation_id,
                request_data,
                response_tx,
                store,
                pending_store
            ),
            NetworkAction::SendStoreResponse { channel, message } => {
                send_protocol_response!(swarm, channel, message, store)
            }
            NetworkAction::SendGetRequest {
                peer,
                addresses,
                operation_id,
                request_data,
                response_tx,
            } => send_protocol_request!(
                swarm,
                self,
                peer,
                addresses,
                operation_id,
                request_data,
                response_tx,
                get,
                pending_get
            ),
            NetworkAction::SendGetResponse { channel, message } => {
                send_protocol_response!(swarm, channel, message, get)
            }
            NetworkAction::SendFinalityRequest {
                peer,
                addresses,
                operation_id,
                request_data,
                response_tx,
            } => send_protocol_request!(
                swarm,
                self,
                peer,
                addresses,
                operation_id,
                request_data,
                response_tx,
                finality,
                pending_finality
            ),
            NetworkAction::SendFinalityResponse { channel, message } => {
                send_protocol_response!(swarm, channel, message, finality)
            }
            NetworkAction::SendBatchGetRequest {
                peer,
                addresses,
                operation_id,
                request_data,
                response_tx,
            } => send_protocol_request!(
                swarm,
                self,
                peer,
                addresses,
                operation_id,
                request_data,
                response_tx,
                batch_get,
                pending_batch_get
            ),
            NetworkAction::SendBatchGetResponse { channel, message } => {
                send_protocol_response!(swarm, channel, message, batch_get)
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
        rx.await.map_err(|_| NetworkError::ResponseChannelClosed)
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
        rx.await.map_err(|_| NetworkError::ResponseChannelClosed)
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
    #[instrument(
        name = "network_store",
        skip(self, addresses, request_data),
        fields(peer_id = %peer, operation_id = %operation_id)
    )]
    pub(crate) async fn send_store_request(
        &self,
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        operation_id: Uuid,
        request_data: StoreRequestData,
    ) -> Result<StoreResponseData, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_action(NetworkAction::SendStoreRequest {
            peer,
            addresses,
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
        addresses: Vec<Multiaddr>,
        operation_id: Uuid,
        request_data: GetRequestData,
    ) -> Result<GetResponseData, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_action(NetworkAction::SendGetRequest {
            peer,
            addresses,
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
        addresses: Vec<Multiaddr>,
        operation_id: Uuid,
        request_data: FinalityRequestData,
    ) -> Result<FinalityResponseData, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_action(NetworkAction::SendFinalityRequest {
            peer,
            addresses,
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
        skip(self, addresses, request_data),
        fields(peer_id = %peer, operation_id = %operation_id)
    )]
    pub(crate) async fn send_batch_get_request(
        &self,
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        operation_id: Uuid,
        request_data: BatchGetRequestData,
    ) -> Result<BatchGetResponseData, NetworkError> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_action(NetworkAction::SendBatchGetRequest {
            peer,
            addresses,
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
