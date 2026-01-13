use libp2p::Multiaddr;
use network::{PeerId, RequestMessage, ResponseMessage, Swarm, SwarmAction, request_response};
use tokio::sync::mpsc;

use crate::{
    network::{Behaviour, NetworkProtocols},
    types::protocol::{GetRequestData, GetResponseData, StoreRequestData, StoreResponseData},
};

pub enum NetworkAction {
    DialClosestPeers(Vec<PeerId>),
    AddKadAddresses {
        peer_id: PeerId,
        listen_addrs: Vec<Multiaddr>,
    },
    SendStoreRequest {
        peer: PeerId,
        message: RequestMessage<StoreRequestData>,
    },
    SendGetRequest {
        peer: PeerId,
        message: RequestMessage<GetRequestData>,
    },
    SendStoreResponse {
        channel: request_response::ResponseChannel<ResponseMessage<StoreResponseData>>,
        message: ResponseMessage<StoreResponseData>,
    },
    SendGetResponse {
        channel: request_response::ResponseChannel<ResponseMessage<GetResponseData>>,
        message: ResponseMessage<GetResponseData>,
    },
}

// Implement SwarmAction trait from network manager crate
impl SwarmAction<Behaviour> for NetworkAction {
    fn apply(self, swarm: &mut Swarm<Behaviour>) {
        match self {
            NetworkAction::DialClosestPeers(peers) => {
                for peer in peers {
                    // Access base protocols directly on NetworkBehaviour
                    swarm.behaviour_mut().kad.get_closest_peers(peer);
                }
            }
            NetworkAction::AddKadAddresses {
                peer_id,
                listen_addrs,
            } => {
                for address in listen_addrs {
                    // Access base protocols directly on NetworkBehaviour
                    swarm.behaviour_mut().kad.add_address(&peer_id, address);
                }
            }
            NetworkAction::SendStoreRequest { peer, message } => {
                // Access app protocols through the 'protocols' field
                let _ = swarm
                    .behaviour_mut()
                    .protocols
                    .store
                    .send_request(&peer, message);
            }
            NetworkAction::SendGetRequest { peer, message } => {
                // Access app protocols through the 'protocols' field
                let _ = swarm
                    .behaviour_mut()
                    .protocols
                    .get
                    .send_request(&peer, message);
            }
            NetworkAction::SendStoreResponse { channel, message } => {
                // Access app protocols through the 'protocols' field
                let _ = swarm
                    .behaviour_mut()
                    .protocols
                    .store
                    .send_response(channel, message);
            }
            NetworkAction::SendGetResponse { channel, message } => {
                // Access app protocols through the 'protocols' field
                let _ = swarm
                    .behaviour_mut()
                    .protocols
                    .get
                    .send_response(channel, message);
            }
        }
    }
}

#[derive(Clone)]
pub struct NetworkHandle {
    action_tx: mpsc::Sender<NetworkAction>,
}

impl NetworkHandle {
    pub fn new(action_tx: mpsc::Sender<NetworkAction>) -> Self {
        Self { action_tx }
    }

    pub async fn dial_peers(
        &self,
        peers: Vec<PeerId>,
    ) -> Result<(), mpsc::error::SendError<NetworkAction>> {
        self.action_tx
            .send(NetworkAction::DialClosestPeers(peers))
            .await
    }

    pub async fn add_kad_addresses(
        &self,
        peer_id: PeerId,
        listen_addrs: Vec<Multiaddr>,
    ) -> Result<(), mpsc::error::SendError<NetworkAction>> {
        self.action_tx
            .send(NetworkAction::AddKadAddresses {
                peer_id,
                listen_addrs,
            })
            .await
    }

    pub async fn send_store_request(
        &self,
        peer: PeerId,
        message: RequestMessage<StoreRequestData>,
    ) -> Result<(), mpsc::error::SendError<NetworkAction>> {
        self.action_tx
            .send(NetworkAction::SendStoreRequest { peer, message })
            .await
    }

    pub async fn send_get_request(
        &self,
        peer: PeerId,
        message: RequestMessage<GetRequestData>,
    ) -> Result<(), mpsc::error::SendError<NetworkAction>> {
        self.action_tx
            .send(NetworkAction::SendGetRequest { peer, message })
            .await
    }

    pub async fn send_store_response(
        &self,
        channel: request_response::ResponseChannel<ResponseMessage<StoreResponseData>>,
        message: ResponseMessage<StoreResponseData>,
    ) -> Result<(), mpsc::error::SendError<NetworkAction>> {
        self.action_tx
            .send(NetworkAction::SendStoreResponse { channel, message })
            .await
    }

    pub async fn send_get_response(
        &self,
        channel: request_response::ResponseChannel<ResponseMessage<GetResponseData>>,
        message: ResponseMessage<GetResponseData>,
    ) -> Result<(), mpsc::error::SendError<NetworkAction>> {
        self.action_tx
            .send(NetworkAction::SendGetResponse { channel, message })
            .await
    }
}
