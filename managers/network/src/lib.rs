pub mod action;
pub mod error;
mod key_manager;
pub mod message;

use action::NetworkAction;
use error::NetworkError;
use key_manager::KeyManager;
use libp2p::futures::StreamExt;
pub use libp2p::identify;
use libp2p::kad::store::MemoryStore;
use libp2p::kad::{BucketInserts, Config as KademliaConfig, Mode};
use libp2p::request_response::ProtocolSupport;
pub use libp2p::swarm::SwarmEvent;
use libp2p::swarm::{derive_prelude::Either, NetworkBehaviour};
use libp2p::{noise, tcp, StreamProtocol, SwarmBuilder};
pub use libp2p::{request_response, PeerId, Swarm};
use libp2p_mplex::MplexConfig;
use message::{RequestMessage, ResponseMessage};
use serde::Deserialize;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tracing::info;
use void::Void;

use crate::message::{GetRequestData, GetResponseData, StoreRequestData, StoreResponseData};

pub type NestedError = Either<Either<Either<std::io::Error, std::io::Error>, Void>, Void>;
pub type SwarmError = Either<NestedError, Void>;
pub type NetworkEvent = SwarmEvent<BehaviourEvent>;

#[derive(Debug, Clone, Deserialize)]
pub struct NetworkManagerConfig {
    port: u32,
    data_folder_path: String,
    bootstrap: Vec<String>,
}

pub struct NetworkManager {
    config: NetworkManagerConfig,
    swarm: Mutex<Swarm<Behaviour>>,
    peer_id: PeerId,
}

impl NetworkManager {
    /// Creates a new NetworkManager instance
    ///
    /// # Errors
    ///
    /// Returns `NetworkError` if:
    /// - Key generation/loading fails
    /// - Bootstrap node parsing fails
    /// - Transport creation fails
    /// - Swarm building fails
    pub async fn new(config: &NetworkManagerConfig) -> Result<Self, NetworkError> {
        // Load or generate keypair
        let key = KeyManager::generate_or_load_key(config.data_folder_path.as_str()).await?;

        let public_key = key.public();
        let local_peer_id = PeerId::from(&public_key);

        info!("Network ID is {}", local_peer_id.to_base58());

        // Create request-response behaviors for Store protocol
        let store_behaviour = request_response::json::Behaviour::<
            RequestMessage<StoreRequestData>,
            ResponseMessage<StoreResponseData>,
        >::new(
            [(StreamProtocol::new("/store/1.0.0"), ProtocolSupport::Full)],
            request_response::Config::default(),
        );

        // Create request-response behaviors for Get protocol
        let get_behaviour = request_response::json::Behaviour::<
            RequestMessage<GetRequestData>,
            ResponseMessage<GetResponseData>,
        >::new(
            [(StreamProtocol::new("/get/1.0.0"), ProtocolSupport::Full)],
            request_response::Config::default(),
        );

        let swarm = {
            // Create a Kademlia behaviour
            let mut cfg = KademliaConfig::default();
            cfg.set_kbucket_inserts(BucketInserts::OnConnected);
            let store = MemoryStore::new(local_peer_id);

            let mut behaviour = Behaviour {
                kad: libp2p::kad::Behaviour::with_config(local_peer_id, store, cfg),
                identify: identify::Behaviour::new(identify::Config::new(
                    "/ipfs/id/1.0.0".to_string(),
                    public_key,
                )),
                ping: libp2p::ping::Behaviour::new(libp2p::ping::Config::default()),
                store: store_behaviour,
                get: get_behaviour,
            };

            behaviour.kad.set_mode(Some(Mode::Server));

            // Parse and add bootstrap nodes
            for bootstrap in &config.bootstrap {
                let parts: Vec<&str> = bootstrap.split("/p2p/").collect();

                if parts.len() != 2 {
                    return Err(NetworkError::InvalidBootstrapNode {
                        expected: "/ip4/.../p2p/...".to_string(),
                        received: bootstrap.to_string(),
                    });
                }

                let bootstrap_peer_id =
                    parts[1].parse().map_err(|e| NetworkError::InvalidPeerId {
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

                behaviour
                    .kad
                    .add_address(&bootstrap_peer_id, bootstrap_address);
            }

            // Build the swarm
            SwarmBuilder::with_existing_identity(key.clone())
                .with_tokio()
                .with_tcp(
                    tcp::Config::default(),
                    noise::Config::new,
                    MplexConfig::default,
                )
                .map_err(NetworkError::TransportCreation)?
                .with_behaviour(|_| behaviour)?
                .build()
        };

        Ok(Self {
            config: config.to_owned(),
            swarm: tokio::sync::Mutex::new(swarm),
            peer_id: local_peer_id,
        })
    }

    /// Starts listening on the configured port and bootstraps the DHT
    ///
    /// # Errors
    ///
    /// Returns `NetworkError` if:
    /// - Multiaddr parsing fails
    /// - Listener initialization fails
    pub async fn start_listening(&self) -> Result<(), NetworkError> {
        let mut locked_swarm = self.swarm.lock().await;

        let listen_addr = format!("/ip4/0.0.0.0/tcp/{}", self.config.port);
        let listen_addr: libp2p::Multiaddr =
            listen_addr
                .parse()
                .map_err(|e| NetworkError::InvalidMultiaddr {
                    parsed: listen_addr,
                    source: e,
                })?;

        locked_swarm
            .listen_on(listen_addr.clone())
            .map_err(|e| NetworkError::ListenerFailed {
                address: listen_addr.to_string(),
                source: Box::new(e),
            })?;

        // Bootstrap the DHT (returns a QueryId, not an error)
        let _ = locked_swarm.behaviour_mut().kad.bootstrap();

        Ok(())
    }

    /// Main event loop that handles network commands and swarm events
    ///
    /// This function runs indefinitely and handles:
    /// - Incoming network actions (store/get requests/responses, peer discovery)
    /// - Swarm events (connections, protocol events, etc.)
    ///
    /// # Errors
    ///
    /// Returns `NetworkError` if:
    /// - Starting the listener fails
    /// - Sending responses fails
    pub async fn listen_and_handle_swarm_events(
        &self,
        mut command_rx: Receiver<NetworkAction>,
        event_tx: Sender<NetworkEvent>,
    ) -> Result<(), NetworkError> {
        self.start_listening().await?;

        let mut locked_swarm = self.swarm.lock().await;
        loop {
            tokio::select! {
                Some(command) = command_rx.recv() => {
                    match command {
                        NetworkAction::StoreRequest { peer, message } => {
                            locked_swarm
                                .behaviour_mut()
                                .store
                                .send_request(&peer, message);
                        }
                        NetworkAction::StoreResponse { channel, message } => {
                            locked_swarm
                                .behaviour_mut()
                                .store
                                .send_response(channel, message)
                                .map_err(|_| NetworkError::ResponseFailed { protocol: "store" })?;

                        }
                        NetworkAction::GetRequest { peer, message } => {
                            locked_swarm
                                .behaviour_mut()
                                .get
                                .send_request(&peer, message);
                        }
                        NetworkAction::GetResponse { channel, message } => {
                            locked_swarm
                                .behaviour_mut()
                                .get
                                .send_response(channel, message)
                                .map_err(|_| NetworkError::ResponseFailed { protocol: "get" })?;

                        }
                        NetworkAction::GetClosestPeers { peer } => {
                            locked_swarm.behaviour_mut().kad.get_closest_peers(peer);
                        }
                        NetworkAction::AddAddress { peer_id, addresses } => {
                            for addr in addresses {
                                locked_swarm.behaviour_mut().kad.add_address(&peer_id, addr);
                            }
                        }
                    }
                }
                event = locked_swarm.select_next_some() => {
                    // Send event to the event channel
                    // If send fails, the receiver is dropped, so we can ignore the error
                    let _ = event_tx.send(event).await;
                }
            }
        }
    }

    /// Returns the peer ID of this node
    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }
}

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    kad: libp2p::kad::Behaviour<MemoryStore>,
    identify: identify::Behaviour,
    ping: libp2p::ping::Behaviour,
    store: libp2p::request_response::json::Behaviour<
        RequestMessage<StoreRequestData>,
        ResponseMessage<StoreResponseData>,
    >,
    get: libp2p::request_response::json::Behaviour<
        RequestMessage<GetRequestData>,
        ResponseMessage<GetResponseData>,
    >,
}
