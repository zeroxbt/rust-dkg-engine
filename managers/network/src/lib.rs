pub mod action;
mod key_manager;
pub mod message;

use action::NetworkAction;
use key_manager::KeyManager;
use libp2p::core::upgrade::Version;
use libp2p::futures::StreamExt;
pub use libp2p::identify;
use libp2p::kad::record::store::MemoryStore;
use libp2p::kad::{Kademlia, KademliaBucketInserts, KademliaConfig, Mode};
use libp2p::request_response::ProtocolSupport;
use libp2p::swarm::{derive_prelude::Either, NetworkBehaviour};
pub use libp2p::swarm::{SwarmBuilder, SwarmEvent};
use libp2p::{noise, tcp, yamux, StreamProtocol, Transport};
pub use libp2p::{request_response, PeerId, Swarm};
use message::{
    GetMessageRequestData, GetMessageResponseData, RequestMessage, ResponseMessage,
    StoreMessageRequestData, StoreMessageResponseData,
};
use serde::Deserialize;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tracing::info;
use void::Void;

pub type NestedError = Either<Either<Either<std::io::Error, std::io::Error>, Void>, Void>;
pub type SwarmError = Either<NestedError, Void>;
pub type NetworkEvent = SwarmEvent<BehaviourEvent, SwarmError>;

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
    pub async fn new(config: &NetworkManagerConfig) -> Self {
        let key = KeyManager::generate_or_load_key(config.data_folder_path.as_str())
            .await
            .unwrap();

        let public_key = key.public();
        let local_peer_id = PeerId::from(&public_key);

        info!("Network ID is {}", local_peer_id.to_base58());

        let transport = tcp::tokio::Transport::default()
            .upgrade(Version::V1Lazy)
            .authenticate(noise::Config::new(&key).expect("unable to create config"))
            .multiplex(yamux::Config::default())
            .boxed();

        let store_behaviour = request_response::json::Behaviour::<
            RequestMessage<StoreMessageRequestData>,
            ResponseMessage<StoreMessageResponseData>,
        >::new(
            [(StreamProtocol::new("/store/1.0.0"), ProtocolSupport::Full)],
            request_response::Config::default(),
        );

        let get_behaviour = request_response::json::Behaviour::<
            RequestMessage<GetMessageRequestData>,
            ResponseMessage<GetMessageResponseData>,
        >::new(
            [(StreamProtocol::new("/get/1.0.0"), ProtocolSupport::Full)],
            request_response::Config::default(),
        );

        let swarm = {
            // Create a Kademlia behaviour.
            let mut cfg = KademliaConfig::default();
            cfg.set_kbucket_inserts(KademliaBucketInserts::OnConnected);
            let store = MemoryStore::new(local_peer_id);

            let mut behaviour = Behaviour {
                kad: Kademlia::with_config(local_peer_id, store, cfg),
                identify: identify::Behaviour::new(identify::Config::new(
                    "/ipfs/id/1.0.0".to_string(),
                    public_key,
                )),
                ping: libp2p::ping::Behaviour::new(libp2p::ping::Config::default()),
                store: store_behaviour,
                get: get_behaviour,
            };

            behaviour.kad.set_mode(Some(Mode::Server));

            config.bootstrap.iter().for_each(|bootstrap| {
                let bootstrap_peer_id = bootstrap.split("/p2p/").last().unwrap().parse().unwrap();
                let bootstrap_address = bootstrap.split("/p2p/").next().unwrap().parse().unwrap();

                behaviour
                    .kad
                    .add_address(&bootstrap_peer_id, bootstrap_address);
            });

            SwarmBuilder::with_tokio_executor(transport, behaviour, local_peer_id).build()
        };

        Self {
            config: config.to_owned(),
            swarm: tokio::sync::Mutex::new(swarm),
            peer_id: local_peer_id,
        }
    }

    pub async fn start_listening(&self) {
        let mut locked_swarm = self.swarm.lock().await;

        locked_swarm
            .listen_on(
                format!("/ip4/0.0.0.0/tcp/{}", self.config.port)
                    .parse()
                    .expect("could not parse multiaddr"),
            )
            .expect("could not initialize swarm listener");

        let _ = locked_swarm.behaviour_mut().kad.bootstrap();
    }

    pub async fn handle_swarm_events(
        &self,
        mut command_rx: Receiver<NetworkAction>,
        event_tx: Sender<NetworkEvent>,
    ) {
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
                        .unwrap();
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
                        .unwrap();
                }
                NetworkAction::GetClosestPeers {peer} => {
                    locked_swarm.behaviour_mut().kad.get_closest_peers(peer);
                },
                NetworkAction::AddAddress {peer_id, addresses} => {
                    addresses.iter().for_each(|addr| {
                        locked_swarm.behaviour_mut().kad.add_address(&peer_id, addr.to_owned());
                    })

                }
            }
                    },
                    event = locked_swarm.select_next_some() => {
                         {
                            let _ = event_tx.send(event).await;
                        }
                    }
                }
        }
    }

    pub fn get_peer_id(&self) -> &PeerId {
        &self.peer_id
    }
}

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    kad: Kademlia<MemoryStore>,
    identify: identify::Behaviour,
    ping: libp2p::ping::Behaviour,
    store: libp2p::request_response::json::Behaviour<
        RequestMessage<StoreMessageRequestData>,
        ResponseMessage<StoreMessageResponseData>,
    >,
    get: libp2p::request_response::json::Behaviour<
        RequestMessage<GetMessageRequestData>,
        ResponseMessage<GetMessageResponseData>,
    >,
}
