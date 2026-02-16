use std::time::Duration;

use libp2p::{Multiaddr, PeerId, StreamProtocol};

#[derive(Debug, Clone)]
pub struct IdentifyInfo {
    pub protocols: Vec<StreamProtocol>,
    pub listen_addrs: Vec<Multiaddr>,
}

#[derive(Debug, Clone)]
pub enum PeerEvent {
    RequestOutcome(RequestOutcome),
    IdentifyReceived { peer_id: PeerId, info: IdentifyInfo },
    KadLookup { target: PeerId, found: bool },
    ConnectionEstablished { peer_id: PeerId },
}

#[derive(Debug, Clone, Copy)]
pub enum RequestOutcomeKind {
    Success,
    ResponseError,
    Failure,
}

#[derive(Debug, Clone)]
pub struct RequestOutcome {
    pub peer_id: PeerId,
    pub protocol: &'static str,
    pub outcome: RequestOutcomeKind,
    pub elapsed: Duration,
}
