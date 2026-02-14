//! NetworkEventLoop - owns the swarm and runs the event loop.
//!
//! This is the internal implementation that handles all network events.
//! It receives actions from NetworkManager handles and processes swarm events.

use std::time::Instant;

use libp2p::{
    Multiaddr, PeerId, Swarm, identify, kad,
    multiaddr::Protocol,
    request_response,
    swarm::{NetworkBehaviour, SwarmEvent},
};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    IdentifyInfo, ImmediateResponse, InboundDecision, JsCompatCodec, NetworkError,
    NetworkEventHandler, NetworkManagerConfig, PeerEvent, PendingRequests, RequestContext,
    RequestMessage, RequestOutcome, RequestOutcomeKind, ResponseMessage,
    actions::{NetworkControlAction, NetworkDataAction},
    behaviour::{NodeBehaviour, NodeBehaviourEvent},
    message::{RequestMessageHeader, RequestMessageType},
    protocols::{
        BatchGetProtocol, BatchGetResponseData, FinalityProtocol, FinalityResponseData,
        GetProtocol, GetResponseData, ProtocolResponse, ProtocolSpec, StoreProtocol,
        StoreResponseData,
    },
};

/// Maximum number of control actions processed per loop tick before yielding to
/// swarm/data processing.
const CONTROL_ACTION_BURST: usize = 16;

/// Handle a protocol event - processes responses and dispatches requests.
fn handle_protocol_event<P, H>(
    event: request_response::Event<RequestMessage<P::RequestData>, ResponseMessage<P::Ack>>,
    swarm: &mut Swarm<NodeBehaviour>,
    pending: &mut PendingRequests<ProtocolResponse<P>>,
    handler: &H,
    peer_event_tx: &broadcast::Sender<PeerEvent>,
    send_response: impl FnOnce(
        &mut Swarm<NodeBehaviour>,
        request_response::ResponseChannel<ResponseMessage<P::Ack>>,
        ResponseMessage<P::Ack>,
    ) -> Result<(), ResponseMessage<P::Ack>>,
) where
    P: ProtocolSpec,
    H: NetworkEventHandler + ProtocolHandler<P>,
{
    match event {
        request_response::Event::Message { message, peer, .. } => match message {
            request_response::Message::Response {
                response,
                request_id,
            } => {
                let outcome_kind = match &response.data {
                    super::message::ResponseBody::Ack(_) => RequestOutcomeKind::Success,
                    super::message::ResponseBody::Error(_) => RequestOutcomeKind::ResponseError,
                };
                if let Some(context) = pending.complete_success(request_id, response.data) {
                    let outcome = RequestOutcome {
                        peer_id: context.peer_id,
                        protocol: context.protocol,
                        outcome: outcome_kind,
                        elapsed: context.started_at.elapsed(),
                    };
                    let _ = peer_event_tx.send(PeerEvent::RequestOutcome(outcome));
                }
            }
            request_response::Message::Request {
                request, channel, ..
            } => match handler.handle_request(request, channel, peer) {
                InboundDecision::Scheduled => {}
                InboundDecision::RespondNow(ImmediateResponse { channel, message }) => {
                    if let Err(response) = send_response(swarm, channel, message) {
                        tracing::warn!(
                            %peer,
                            response_header = ?response.header,
                            "{} immediate response could not be sent",
                            P::NAME
                        );
                    }
                }
            },
        },
        request_response::Event::OutboundFailure {
            request_id,
            error,
            peer,
            ..
        } => {
            tracing::warn!(%peer, ?request_id, ?error, "{} request failed", P::NAME);
            if let Some(context) = pending.complete_failure(request_id, NetworkError::from(&error))
            {
                let outcome = RequestOutcome {
                    peer_id: context.peer_id,
                    protocol: context.protocol,
                    outcome: RequestOutcomeKind::Failure,
                    elapsed: context.started_at.elapsed(),
                };
                let _ = peer_event_tx.send(PeerEvent::RequestOutcome(outcome));
            }
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
                "Failed to respond to {} request",
                P::NAME
            );
        }
        request_response::Event::ResponseSent {
            peer, request_id, ..
        } => {
            tracing::trace!(%peer, ?request_id, "{} response sent", P::NAME);
        }
    }
}

/// Trait for handling protocol-specific inbound requests.
trait ProtocolHandler<P: ProtocolSpec>: Send + Sync {
    fn handle_request(
        &self,
        request: RequestMessage<P::RequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<P::Ack>>,
        peer: PeerId,
    ) -> InboundDecision<P::Ack>;
}

impl<H: NetworkEventHandler> ProtocolHandler<StoreProtocol> for H {
    fn handle_request(
        &self,
        request: RequestMessage<<StoreProtocol as ProtocolSpec>::RequestData>,
        channel: request_response::ResponseChannel<
            ResponseMessage<<StoreProtocol as ProtocolSpec>::Ack>,
        >,
        peer: PeerId,
    ) -> InboundDecision<<StoreProtocol as ProtocolSpec>::Ack> {
        self.on_store_request(request, channel, peer)
    }
}

impl<H: NetworkEventHandler> ProtocolHandler<GetProtocol> for H {
    fn handle_request(
        &self,
        request: RequestMessage<<GetProtocol as ProtocolSpec>::RequestData>,
        channel: request_response::ResponseChannel<
            ResponseMessage<<GetProtocol as ProtocolSpec>::Ack>,
        >,
        peer: PeerId,
    ) -> InboundDecision<<GetProtocol as ProtocolSpec>::Ack> {
        self.on_get_request(request, channel, peer)
    }
}

impl<H: NetworkEventHandler> ProtocolHandler<FinalityProtocol> for H {
    fn handle_request(
        &self,
        request: RequestMessage<<FinalityProtocol as ProtocolSpec>::RequestData>,
        channel: request_response::ResponseChannel<
            ResponseMessage<<FinalityProtocol as ProtocolSpec>::Ack>,
        >,
        peer: PeerId,
    ) -> InboundDecision<<FinalityProtocol as ProtocolSpec>::Ack> {
        self.on_finality_request(request, channel, peer)
    }
}

impl<H: NetworkEventHandler> ProtocolHandler<BatchGetProtocol> for H {
    fn handle_request(
        &self,
        request: RequestMessage<<BatchGetProtocol as ProtocolSpec>::RequestData>,
        channel: request_response::ResponseChannel<
            ResponseMessage<<BatchGetProtocol as ProtocolSpec>::Ack>,
        >,
        peer: PeerId,
    ) -> InboundDecision<<BatchGetProtocol as ProtocolSpec>::Ack> {
        self.on_batch_get_request(request, channel, peer)
    }
}

/// Send a protocol request via the behaviour.
fn send_protocol_request<P, B>(
    swarm: &mut Swarm<NodeBehaviour>,
    pending: &mut PendingRequests<ProtocolResponse<P>>,
    peer: PeerId,
    operation_id: Uuid,
    request_data: P::RequestData,
    response_tx: oneshot::Sender<Result<ProtocolResponse<P>, NetworkError>>,
    behaviour_accessor: B,
) where
    P: ProtocolSpec,
    B: FnOnce(
        &mut NodeBehaviour,
    ) -> &mut request_response::Behaviour<
        JsCompatCodec<RequestMessage<P::RequestData>, ResponseMessage<P::Ack>>,
    >,
{
    let message = RequestMessage {
        header: RequestMessageHeader::new(operation_id, RequestMessageType::ProtocolRequest),
        data: request_data,
    };
    let started_at = Instant::now();
    let request_id = behaviour_accessor(swarm.behaviour_mut()).send_request(&peer, message);
    pending.insert_sender(
        request_id,
        response_tx,
        RequestContext {
            peer_id: peer,
            protocol: P::NAME,
            started_at,
        },
    );
}

fn strip_p2p_protocol(addr: &Multiaddr) -> Multiaddr {
    addr.into_iter()
        .filter(|proto| !matches!(proto, Protocol::P2p(_)))
        .collect()
}

fn dial_peer_if_needed(swarm: &mut Swarm<NodeBehaviour>, peer: PeerId) {
    if swarm.is_connected(&peer) {
        tracing::trace!(%peer, "already connected to peer, skipping dial");
    } else if let Err(e) = swarm.dial(peer) {
        tracing::debug!(%peer, error = %e, "failed to dial peer");
    } else {
        tracing::debug!(%peer, "dialing peer discovered via DHT");
    }
}

/// NetworkEventLoop owns the swarm and runs the event loop.
///
/// This is created by `NetworkManager::connect()` and runs in its own task.
pub(crate) struct NetworkEventLoop {
    swarm: Swarm<NodeBehaviour>,
    control_rx: mpsc::Receiver<NetworkControlAction>,
    data_rx: mpsc::Receiver<NetworkDataAction>,
    shutdown: CancellationToken,
    config: NetworkManagerConfig,
    peer_event_tx: broadcast::Sender<PeerEvent>,
    // Per-protocol pending request tracking
    pending_store: PendingRequests<StoreResponseData>,
    pending_get: PendingRequests<GetResponseData>,
    pending_finality: PendingRequests<FinalityResponseData>,
    pending_batch_get: PendingRequests<BatchGetResponseData>,
}

impl NetworkEventLoop {
    /// Create a new NetworkEventLoop.
    pub(super) fn new(
        swarm: Swarm<NodeBehaviour>,
        control_rx: mpsc::Receiver<NetworkControlAction>,
        data_rx: mpsc::Receiver<NetworkDataAction>,
        config: NetworkManagerConfig,
        peer_event_tx: broadcast::Sender<PeerEvent>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            swarm,
            control_rx,
            data_rx,
            shutdown,
            config,
            peer_event_tx,
            pending_store: PendingRequests::new(),
            pending_get: PendingRequests::new(),
            pending_finality: PendingRequests::new(),
            pending_batch_get: PendingRequests::new(),
        }
    }

    /// Starts listening on the configured port.
    ///
    /// # Errors
    /// Returns `NetworkError` if listener initialization fails.
    pub(crate) fn start_listening(&mut self) -> Result<(), NetworkError> {
        let listen_addr = format!("/ip4/0.0.0.0/tcp/{}", self.config.port);
        let listen_addr: Multiaddr =
            listen_addr
                .parse()
                .map_err(|e| NetworkError::InvalidMultiaddr {
                    parsed: listen_addr,
                    source: e,
                })?;

        self.swarm
            .listen_on(listen_addr.clone())
            .map_err(|e| NetworkError::ListenerFailed {
                address: listen_addr.to_string(),
                source: Box::new(e),
            })?;

        Ok(())
    }

    fn run_bootstrap(&mut self) {
        if self.config.bootstrap.is_empty() {
            tracing::info!("No bootstrap nodes configured");
        } else {
            for bootstrap in &self.config.bootstrap {
                match bootstrap.parse::<Multiaddr>() {
                    Ok(addr) => {
                        if let Err(err) = self.swarm.dial(addr.clone()) {
                            tracing::warn!(
                                address = %addr,
                                error = %err,
                                "Failed to dial bootstrap node"
                            );
                        } else {
                            tracing::info!(address = %addr, "Dialing bootstrap node");
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            address = %bootstrap,
                            error = %err,
                            "Invalid bootstrap multiaddr"
                        );
                    }
                }
            }
        }

        let has_known_peers = self.swarm.behaviour_mut().kad.kbuckets().next().is_some();
        if !has_known_peers {
            tracing::debug!("Kademlia bootstrap skipped: no known peers");
            return;
        }

        match self.swarm.behaviour_mut().kad.bootstrap() {
            Ok(query_id) => {
                tracing::info!(?query_id, "Started Kademlia bootstrap");
            }
            Err(err) => {
                tracing::warn!(error = %err, "Failed to start Kademlia bootstrap");
            }
        }
    }

    /// Runs the network event loop, processing swarm events and actions.
    ///
    /// This method consumes self and runs until shutdown is requested or both
    /// control/data channels are closed.
    pub(crate) async fn run<H: NetworkEventHandler>(mut self, handler: &H) {
        use libp2p::futures::StreamExt;

        self.run_bootstrap();

        let mut control_closed = false;
        let mut data_closed = false;

        loop {
            if self.shutdown.is_cancelled() {
                tracing::info!("Network shutdown token cancelled, stopping network loop");
                break;
            }

            if control_closed && data_closed {
                tracing::info!("Control and data channels closed, shutting down network loop");
                break;
            }

            // Drain a small burst of control actions first to keep control-plane
            // signals responsive under data-plane load.
            for _ in 0..CONTROL_ACTION_BURST {
                if control_closed {
                    break;
                }

                match self.control_rx.try_recv() {
                    Ok(action) => self.handle_control_action(action),
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        tracing::info!("Control channel closed");
                        control_closed = true;
                        break;
                    }
                }
            }

            tokio::select! {
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event, handler);
                }
                _ = self.shutdown.cancelled() => {
                    tracing::info!("Network shutdown token cancelled, stopping network loop");
                    break;
                }
                control_action = self.control_rx.recv(), if !control_closed => {
                    match control_action {
                        Some(action) => self.handle_control_action(action),
                        None => {
                            tracing::info!("Control channel closed");
                            control_closed = true;
                        }
                    }
                }
                data_action = self.data_rx.recv(), if !data_closed => {
                    match data_action {
                        Some(action) => self.handle_data_action(action),
                        None => {
                            tracing::info!("Data channel closed");
                            data_closed = true;
                        }
                    }
                }
            }
        }
    }

    /// Process a swarm event.
    fn handle_swarm_event<H: NetworkEventHandler>(
        &mut self,
        event: SwarmEvent<<NodeBehaviour as NetworkBehaviour>::ToSwarm>,
        handler: &H,
    ) {
        match event {
            SwarmEvent::Behaviour(behaviour_event) => match behaviour_event {
                // Protocol events
                NodeBehaviourEvent::Store(inner) => {
                    handle_protocol_event::<StoreProtocol, _>(
                        inner,
                        &mut self.swarm,
                        &mut self.pending_store,
                        handler,
                        &self.peer_event_tx,
                        |swarm, channel, message| {
                            swarm.behaviour_mut().store.send_response(channel, message)
                        },
                    );
                }
                NodeBehaviourEvent::Get(inner) => {
                    handle_protocol_event::<GetProtocol, _>(
                        inner,
                        &mut self.swarm,
                        &mut self.pending_get,
                        handler,
                        &self.peer_event_tx,
                        |swarm, channel, message| {
                            swarm.behaviour_mut().get.send_response(channel, message)
                        },
                    );
                }
                NodeBehaviourEvent::Finality(inner) => {
                    handle_protocol_event::<FinalityProtocol, _>(
                        inner,
                        &mut self.swarm,
                        &mut self.pending_finality,
                        handler,
                        &self.peer_event_tx,
                        |swarm, channel, message| {
                            swarm
                                .behaviour_mut()
                                .finality
                                .send_response(channel, message)
                        },
                    );
                }
                NodeBehaviourEvent::BatchGet(inner) => {
                    handle_protocol_event::<BatchGetProtocol, _>(
                        inner,
                        &mut self.swarm,
                        &mut self.pending_batch_get,
                        handler,
                        &self.peer_event_tx,
                        |swarm, channel, message| {
                            swarm
                                .behaviour_mut()
                                .batch_get
                                .send_response(channel, message)
                        },
                    );
                }

                // Identify protocol
                NodeBehaviourEvent::Identify(identify::Event::Received {
                    peer_id, info, ..
                }) => {
                    let identify_info = IdentifyInfo {
                        protocols: info.protocols,
                        listen_addrs: info.listen_addrs,
                    };
                    for addr in identify_info.listen_addrs.iter() {
                        let addr = strip_p2p_protocol(addr);
                        self.swarm.behaviour_mut().kad.add_address(&peer_id, addr);
                    }
                    let _ = self.peer_event_tx.send(PeerEvent::IdentifyReceived {
                        peer_id,
                        info: identify_info,
                    });
                }

                // Kademlia protocol
                NodeBehaviourEvent::Kad(kad::Event::OutboundQueryProgressed {
                    result: kad::QueryResult::GetClosestPeers(result),
                    ..
                }) => match result {
                    Ok(kad::GetClosestPeersOk { key, peers }) => {
                        if let Ok(target) = PeerId::from_bytes(&key) {
                            if peers.iter().any(|p| p.peer_id == target) {
                                let _ = self.peer_event_tx.send(PeerEvent::KadLookup {
                                    target,
                                    found: true,
                                });
                                dial_peer_if_needed(&mut self.swarm, target);
                            } else {
                                let _ = self.peer_event_tx.send(PeerEvent::KadLookup {
                                    target,
                                    found: false,
                                });
                            }
                        }
                    }
                    Err(kad::GetClosestPeersError::Timeout { key, peers }) => {
                        if let Ok(target) = PeerId::from_bytes(&key) {
                            if peers.iter().any(|p| p.peer_id == target) {
                                let _ = self.peer_event_tx.send(PeerEvent::KadLookup {
                                    target,
                                    found: true,
                                });
                                dial_peer_if_needed(&mut self.swarm, target);
                            } else {
                                let _ = self.peer_event_tx.send(PeerEvent::KadLookup {
                                    target,
                                    found: false,
                                });
                            }
                        }
                    }
                },

                _ => {}
            },

            SwarmEvent::NewListenAddr { address, .. } => {
                tracing::info!("Listening on {}", address);
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                let _ = self
                    .peer_event_tx
                    .send(PeerEvent::ConnectionEstablished { peer_id });
            }
            _ => {}
        }
    }

    /// Handle a control-plane action.
    fn handle_control_action(&mut self, action: NetworkControlAction) {
        match action {
            NetworkControlAction::FindPeers(peers) => {
                for peer in peers {
                    self.swarm.behaviour_mut().kad.get_closest_peers(peer);
                }
            }
            NetworkControlAction::GetConnectedPeers(response_tx) => {
                let peers: Vec<PeerId> = self.swarm.connected_peers().copied().collect();
                let _ = response_tx.send(peers);
            }
            NetworkControlAction::AddAddresses { addresses } => {
                let mut total_addrs = 0usize;
                for (peer_id, addrs) in &addresses {
                    for addr in addrs {
                        let addr = strip_p2p_protocol(addr);
                        self.swarm.behaviour_mut().kad.add_address(peer_id, addr);
                        total_addrs += 1;
                    }
                }
                tracing::info!(
                    peers = addresses.len(),
                    addresses = total_addrs,
                    "Added persisted peer addresses to Kademlia"
                );
            }
        }
    }

    /// Handle a data-plane protocol action.
    fn handle_data_action(&mut self, action: NetworkDataAction) {
        match action {
            NetworkDataAction::StoreRequest {
                peer,
                operation_id,
                request_data,
                response_tx,
            } => send_protocol_request::<StoreProtocol, _>(
                &mut self.swarm,
                &mut self.pending_store,
                peer,
                operation_id,
                request_data,
                response_tx,
                |b| &mut b.store,
            ),
            NetworkDataAction::StoreResponse { channel, message } => {
                let _ = self
                    .swarm
                    .behaviour_mut()
                    .store
                    .send_response(channel, message);
            }
            NetworkDataAction::GetRequest {
                peer,
                operation_id,
                request_data,
                response_tx,
            } => send_protocol_request::<GetProtocol, _>(
                &mut self.swarm,
                &mut self.pending_get,
                peer,
                operation_id,
                request_data,
                response_tx,
                |b| &mut b.get,
            ),
            NetworkDataAction::GetResponse { channel, message } => {
                let _ = self
                    .swarm
                    .behaviour_mut()
                    .get
                    .send_response(channel, message);
            }
            NetworkDataAction::FinalityRequest {
                peer,
                operation_id,
                request_data,
                response_tx,
            } => send_protocol_request::<FinalityProtocol, _>(
                &mut self.swarm,
                &mut self.pending_finality,
                peer,
                operation_id,
                request_data,
                response_tx,
                |b| &mut b.finality,
            ),
            NetworkDataAction::FinalityResponse { channel, message } => {
                let _ = self
                    .swarm
                    .behaviour_mut()
                    .finality
                    .send_response(channel, message);
            }
            NetworkDataAction::BatchGetRequest {
                peer,
                operation_id,
                request_data,
                response_tx,
            } => send_protocol_request::<BatchGetProtocol, _>(
                &mut self.swarm,
                &mut self.pending_batch_get,
                peer,
                operation_id,
                request_data,
                response_tx,
                |b| &mut b.batch_get,
            ),
            NetworkDataAction::BatchGetResponse { channel, message } => {
                let _ = self
                    .swarm
                    .behaviour_mut()
                    .batch_get
                    .send_response(channel, message);
            }
        }
    }
}
