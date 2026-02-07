use std::time::Duration;

use libp2p::{Multiaddr, PeerId, StreamProtocol};

#[derive(Debug, Clone)]
pub(crate) struct IdentifyInfo {
    pub protocols: Vec<StreamProtocol>,
    pub listen_addrs: Vec<Multiaddr>,
}

#[derive(Debug, Clone)]
pub(crate) enum PeerEvent {
    RequestOutcome(RequestOutcome),
    IdentifyReceived { peer_id: PeerId, info: IdentifyInfo },
    KadLookup { target: PeerId, found: bool },
    ConnectionEstablished { peer_id: PeerId },
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum RequestOutcomeKind {
    Success,
    ResponseError,
    Failure,
}

#[derive(Debug, Clone)]
pub(crate) struct RequestOutcome {
    pub(crate) peer_id: PeerId,
    pub(crate) protocol: &'static str,
    pub(crate) outcome: RequestOutcomeKind,
    pub(crate) elapsed: Duration,
}
