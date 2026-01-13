pub mod error;
mod key_manager;
pub mod message;

use std::path::PathBuf;

use error::NetworkError;
use key_manager::KeyManager;
pub use libp2p::request_response::ProtocolSupport;
// Re-export libp2p types and identity for application use
pub use libp2p::{
    PeerId, StreamProtocol, Swarm, identify, identity,
    kad::{self, BucketInserts, Config as KademliaConfig, Mode, store::MemoryStore},
    ping, request_response,
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

/// Trait for actions that can be applied to a swarm
///
/// The application layer implements this trait for its action types,
/// allowing the network manager to remain generic over message types.
pub trait SwarmAction<B>: Send
where
    B: NetworkBehaviour,
{
    /// Apply this action to the swarm
    fn apply(self, swarm: &mut Swarm<B>);
}

pub type NestedError = Either<Either<Either<std::io::Error, std::io::Error>, Void>, Void>;
pub type SwarmError = Either<NestedError, Void>;

#[derive(Debug, Clone, Deserialize)]
pub struct NetworkManagerConfig {
    port: u32,
    data_folder_path: PathBuf,
    bootstrap: Vec<String>,
}

/// Hierarchical network behaviour that combines base protocols with app-specific protocols
///
/// NetworkManager provides the base protocols (kad, identify, ping) and the application
/// provides its custom protocols via the generic parameter B.
#[derive(NetworkBehaviour)]
pub struct NestedBehaviour<B: NetworkBehaviour> {
    pub kad: kad::Behaviour<MemoryStore>,
    pub identify: identify::Behaviour,
    pub ping: ping::Behaviour,
    pub protocols: B,
}

/// Generic NetworkManager that works with any application-defined Behaviour
///
/// The application layer defines the Behaviour struct using re-exported libp2p types,
/// including whatever protocols it needs (store, get, custom protocols, etc.)
pub struct NetworkManager<B>
where
    B: NetworkBehaviour,
{
    config: NetworkManagerConfig,
    swarm: Mutex<Swarm<NestedBehaviour<B>>>,
    peer_id: PeerId,
}

impl<B> NetworkManager<B>
where
    B: NetworkBehaviour,
{
    /// Creates a new NetworkManager instance with application-defined behaviour
    ///
    /// NetworkManager creates and configures the base protocols (kad, identify, ping)
    /// and handles bootstrap configuration. The Builder constructs the complete Behaviour
    /// including app-specific protocols.
    ///
    /// # Errors
    ///
    /// Returns `NetworkError` if:
    /// - Key generation/loading fails
    /// - Bootstrap node parsing fails
    /// - Transport creation fails
    /// - Swarm building fails
    pub async fn new(config: &NetworkManagerConfig, behaviour: B) -> Result<Self, NetworkError> {
        // Load or generate keypair
        let key = KeyManager::generate_or_load_key(&config.data_folder_path).await?;

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

        // 3. Ping protocol
        let ping = ping::Behaviour::new(ping::Config::default());

        // Application creates its Behaviour with base protocols + app-specific protocols
        // let behaviour = Builder::build(kad, identify, ping);

        let custom_behaviour = NestedBehaviour::<B> {
            kad,
            identify,
            ping,
            protocols: behaviour,
        };

        // Build the swarm
        let swarm = SwarmBuilder::with_existing_identity(key.clone())
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                libp2p_mplex::Config::default,
            )
            .map_err(NetworkError::TransportCreation)?
            .with_behaviour(|_| custom_behaviour)?
            .build();

        Ok(Self {
            config: config.to_owned(),
            swarm: tokio::sync::Mutex::new(swarm),
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
    /// This method consumes the NetworkManager and runs the event loop.
    /// It receives actions from the action_rx channel and sends events to the event_tx channel.
    ///
    /// # Parameters
    /// - `action_rx`: Channel receiver for incoming network actions
    /// - `event_tx`: Channel sender for outgoing swarm events
    ///
    /// # Type Parameters
    /// - `A`: Action type that implements SwarmAction<NestedBehaviour<B>>
    pub async fn run<A>(
        &self,
        mut action_rx: mpsc::Receiver<A>,
        event_tx: mpsc::Sender<SwarmEvent<<NestedBehaviour<B> as NetworkBehaviour>::ToSwarm>>,
    ) where
        A: SwarmAction<NestedBehaviour<B>>,
    {
        use libp2p::futures::StreamExt;

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
                        Some(action) => {
                            // Apply action to swarm
                           action.apply(&mut swarm);
                        }
                        None => {
                            tracing::error!("Action channel closed, shutting down network loop");
                            break;
                        }
                    }
                }
            }
        }
    }
}
