pub mod error;
pub mod key_manager;
pub mod message;

use std::time::Duration;

use error::NetworkError;
pub use key_manager::KeyManager;
pub use libp2p::request_response::ProtocolSupport;
// Re-export libp2p types and identity for application use
pub use libp2p::{
    Multiaddr, PeerId, StreamProtocol, Swarm, identify, identity,
    kad::{self, BucketInserts, Config as KademliaConfig, Mode, store::MemoryStore},
    request_response,
    swarm::{NetworkBehaviour, SwarmEvent},
};
// Internal libp2p imports for NetworkManager implementation
use libp2p::{SwarmBuilder, noise, swarm::derive_prelude::Either, tcp};
// Re-export message types
pub use message::{ErrorMessage, RequestMessage, ResponseMessage};
use serde::Deserialize;
use tokio::sync::{Mutex, mpsc};
use tracing::info;
use void::Void;

/// Trait for dispatching application protocol messages through the network manager.
pub trait ProtocolDispatch {
    type Request: Send;
    type Response: Send;

    /// Send a request and return the OutboundRequestId for tracking.
    fn send_request(&mut self, request: Self::Request) -> request_response::OutboundRequestId;
    fn send_response(&mut self, response: Self::Response);
}

enum NetworkAction<B>
where
    B: ProtocolDispatch,
{
    /// Discover peers via Kademlia DHT lookup (get_closest_peers).
    /// This queries the network for nodes closest to the given peer IDs,
    /// which populates the routing table with their addresses.
    FindPeers(Vec<PeerId>),
    /// Directly dial a peer (requires addresses to be known in Kademlia).
    DialPeer(PeerId),
    /// Get the list of currently connected peers.
    GetConnectedPeers(tokio::sync::oneshot::Sender<Vec<PeerId>>),
    AddKadAddresses {
        peer_id: PeerId,
        listen_addrs: Vec<Multiaddr>,
    },
    SendProtocolRequest {
        request: B::Request,
        /// Channel to return the OutboundRequestId for tracking
        response_tx: tokio::sync::oneshot::Sender<request_response::OutboundRequestId>,
    },
    SendProtocolResponse {
        response: B::Response,
    },
}
pub type NestedError = Either<Either<Either<std::io::Error, std::io::Error>, Void>, Void>;
pub type SwarmError = Either<NestedError, Void>;

#[derive(Debug, Clone, Deserialize)]
pub struct NetworkManagerConfig {
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

/// Hierarchical network behaviour that combines base protocols with app-specific protocols
///
/// NetworkManager provides the base protocols (kad, identify, ping) and the application
/// provides its custom protocols via the generic parameter B.
#[derive(NetworkBehaviour)]
pub struct NestedBehaviour<B: NetworkBehaviour> {
    pub kad: kad::Behaviour<MemoryStore>,
    pub identify: identify::Behaviour,
    pub protocols: B,
}

/// Generic NetworkManager that works with any application-defined Behaviour
///
/// The application layer defines the Behaviour struct using re-exported libp2p types,
/// including whatever protocols it needs (store, get, custom protocols, etc.)
pub struct NetworkManager<B>
where
    B: NetworkBehaviour + ProtocolDispatch,
{
    config: NetworkManagerConfig,
    swarm: Mutex<Swarm<NestedBehaviour<B>>>,
    action_tx: mpsc::Sender<NetworkAction<B>>,
    action_rx: Mutex<Option<mpsc::Receiver<NetworkAction<B>>>>,
    peer_id: PeerId,
}

impl<B> NetworkManager<B>
where
    B: NetworkBehaviour + ProtocolDispatch,
{
    /// Creates a new NetworkManager instance with application-defined behaviour
    ///
    /// NetworkManager creates and configures the base protocols (kad, identify, ping)
    /// and handles bootstrap configuration. The Builder constructs the complete Behaviour
    /// including app-specific protocols.
    ///
    /// # Arguments
    /// * `config` - Network configuration (port, bootstrap nodes)
    /// * `key` - Pre-loaded libp2p identity keypair
    /// * `behaviour` - Application-defined protocol behaviour
    ///
    /// # Errors
    ///
    /// Returns `NetworkError` if:
    /// - Bootstrap node parsing fails
    /// - Transport creation fails
    /// - Swarm building fails
    pub async fn connect(
        config: &NetworkManagerConfig,
        key: identity::Keypair,
        behaviour: B,
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

        // Application creates its Behaviour with base protocols + app-specific protocols
        // let behaviour = Builder::build(kad, identify, ping);

        let custom_behaviour = NestedBehaviour::<B> {
            kad,
            identify,
            protocols: behaviour,
        };

        // Build the swarm with configurable idle connection timeout.
        // Default libp2p timeout is 10 seconds, which causes connections to drop quickly.
        // We use a configurable timeout (default 5 minutes) to maintain shard table connections.
        let idle_timeout = Duration::from_secs(config.idle_connection_timeout_secs);
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
        })
    }

    /// Starts listening on the configured port
    ///
    /// # Errors
    ///
    /// Returns `NetworkError` if:
    /// - Multiaddr parsing fails
    /// - Listener initialization fails
    pub async fn start_listening(&self) -> Result<(), NetworkError> {
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
    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    /// Runs the network event loop, processing swarm events and actions
    ///
    /// This method runs the event loop using the internal swarm and action channel.
    /// It receives actions from the internal action channel and sends events to the event_tx
    /// channel.
    ///
    /// # Parameters
    /// - `event_tx`: Channel sender for outgoing swarm events
    pub async fn run(
        &self,
        event_tx: mpsc::Sender<SwarmEvent<<NestedBehaviour<B> as NetworkBehaviour>::ToSwarm>>,
    ) {
        use libp2p::futures::StreamExt;

        let mut action_rx = match self.action_rx.lock().await.take() {
            Some(action_rx) => action_rx,
            None => {
                tracing::error!("Action receiver already taken, shutting down network loop");
                return;
            }
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
                        Some(action) => match action {
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
                            NetworkAction::AddKadAddresses {
                                peer_id,
                                listen_addrs,
                            } => {
                                for address in listen_addrs {
                                    swarm.behaviour_mut().kad.add_address(&peer_id, address);
                                }
                            }
                            NetworkAction::SendProtocolRequest { request, response_tx } => {
                                let request_id = swarm.behaviour_mut().protocols.send_request(request);
                                // Send back the request_id for tracking (ignore if receiver dropped)
                                let _ = response_tx.send(request_id);
                            }
                            NetworkAction::SendProtocolResponse { response } => {
                                swarm.behaviour_mut().protocols.send_response(response);
                            }
                        },
                        None => {
                            tracing::error!("Action channel closed, shutting down network loop");
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn enqueue_action(&self, action: NetworkAction<B>) -> Result<(), NetworkError> {
        self.action_tx
            .send(action)
            .await
            .map_err(|_| NetworkError::ActionChannelClosed)
    }

    /// Discover peers via Kademlia DHT lookup.
    /// This initiates a get_closest_peers query for each peer, which will discover
    /// their addresses through the DHT and add them to the routing table.
    pub async fn find_peers(&self, peers: Vec<PeerId>) -> Result<(), NetworkError> {
        self.enqueue_action(NetworkAction::FindPeers(peers)).await
    }

    /// Directly dial a peer to establish a connection.
    /// The peer's addresses must already be known (e.g., from a previous DHT lookup).
    pub async fn dial_peer(&self, peer: PeerId) -> Result<(), NetworkError> {
        self.enqueue_action(NetworkAction::DialPeer(peer)).await
    }

    /// Get the list of currently connected peers.
    pub async fn connected_peers(&self) -> Result<Vec<PeerId>, NetworkError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.enqueue_action(NetworkAction::GetConnectedPeers(tx))
            .await?;
        rx.await.map_err(|_| NetworkError::RequestIdChannelClosed)
    }

    /// Enqueue Kademlia address updates for a peer.
    pub async fn add_kad_addresses(
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

    /// Enqueue a protocol request for dispatch by the swarm.
    /// Returns the OutboundRequestId which can be used to track the request
    /// and correlate it with timeout/response events.
    pub async fn send_protocol_request(
        &self,
        request: B::Request,
    ) -> Result<request_response::OutboundRequestId, NetworkError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.enqueue_action(NetworkAction::SendProtocolRequest {
            request,
            response_tx,
        })
        .await?;
        response_rx
            .await
            .map_err(|_| NetworkError::RequestIdChannelClosed)
    }

    /// Enqueue a protocol response for dispatch by the swarm.
    pub async fn send_protocol_response(&self, response: B::Response) -> Result<(), NetworkError> {
        self.enqueue_action(NetworkAction::SendProtocolResponse { response })
            .await
    }
}
