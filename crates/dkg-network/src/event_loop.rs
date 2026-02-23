//! NetworkEventLoop - owns the swarm and runs the event loop.
//!
//! This is the internal implementation that handles all network events.
//! It receives actions from NetworkManager handles and processes swarm events.

use std::{collections::HashSet, time::Instant};

use dkg_observability as observability;
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
    IdentifyInfo, InboundDecision, InboundRequest, NetworkError, NetworkEventHandler,
    NetworkManagerConfig, PeerEvent, RequestOutcome, RequestOutcomeKind, ResponseHandle,
    actions::{NetworkControlAction, NetworkDataAction},
    behaviour::{NodeBehaviour, NodeBehaviourEvent},
    handle::ACTION_CHANNEL_CAPACITY,
    message::{ProtocolResponse as ResponsePayload, RequestMessage, ResponseMessage},
    pending_requests::{PendingRequests, RequestContext},
    protocols::{
        BatchGetProtocol, BatchGetResponseData, FinalityProtocol, FinalityResponseData,
        GetProtocol, GetResponseData, JsCompatCodec, ProtocolResponse, ProtocolSpec, StoreProtocol,
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
                    ResponsePayload::Ack(_) => RequestOutcomeKind::Success,
                    ResponsePayload::Error(_) => RequestOutcomeKind::ResponseError,
                };
                if let Some(context) = pending.complete_success(request_id, response.data) {
                    let elapsed = context.started_at.elapsed();
                    let outcome_label = match outcome_kind {
                        RequestOutcomeKind::Success => "success",
                        RequestOutcomeKind::ResponseError => "response_error",
                        RequestOutcomeKind::Failure => "failure",
                    };
                    observability::record_network_outbound_request(
                        context.protocol,
                        outcome_label,
                        elapsed,
                    );
                    let outcome = RequestOutcome {
                        peer_id: context.peer_id,
                        protocol: context.protocol,
                        outcome: outcome_kind,
                        elapsed,
                    };
                    let _ = peer_event_tx.send(PeerEvent::RequestOutcome(outcome));
                }
                observability::record_network_pending_requests(P::NAME, pending.len());
            }
            request_response::Message::Request {
                request, channel, ..
            } => {
                let operation_id = request.header.operation_id();
                let inbound_request = InboundRequest::new(operation_id, peer, request.data);
                let response = ResponseHandle::new(channel);

                match handler.handle_request(inbound_request, response) {
                    InboundDecision::Scheduled => {}
                    InboundDecision::RespondNow(immediate_response) => {
                        let (response, message) = immediate_response.into_parts();
                        if let Err(response) = send_response(swarm, response.into_inner(), message)
                        {
                            observability::record_network_response_send(
                                P::NAME,
                                "immediate",
                                "failed",
                            );
                            tracing::warn!(
                                %peer,
                                response_header = ?response.header,
                                "{} immediate response could not be sent",
                                P::NAME
                            );
                        } else {
                            observability::record_network_response_send(P::NAME, "immediate", "ok");
                        }
                    }
                }
            }
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
                observability::record_network_outbound_request(
                    context.protocol,
                    "failure",
                    context.started_at.elapsed(),
                );
                let outcome = RequestOutcome {
                    peer_id: context.peer_id,
                    protocol: context.protocol,
                    outcome: RequestOutcomeKind::Failure,
                    elapsed: context.started_at.elapsed(),
                };
                let _ = peer_event_tx.send(PeerEvent::RequestOutcome(outcome));
            }
            observability::record_network_pending_requests(P::NAME, pending.len());
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
        request: InboundRequest<P::RequestData>,
        response: ResponseHandle<P::Ack>,
    ) -> InboundDecision<P::Ack>;
}

impl<H: NetworkEventHandler> ProtocolHandler<StoreProtocol> for H {
    fn handle_request(
        &self,
        request: InboundRequest<<StoreProtocol as ProtocolSpec>::RequestData>,
        response: ResponseHandle<<StoreProtocol as ProtocolSpec>::Ack>,
    ) -> InboundDecision<<StoreProtocol as ProtocolSpec>::Ack> {
        self.on_store_request(request, response)
    }
}

impl<H: NetworkEventHandler> ProtocolHandler<GetProtocol> for H {
    fn handle_request(
        &self,
        request: InboundRequest<<GetProtocol as ProtocolSpec>::RequestData>,
        response: ResponseHandle<<GetProtocol as ProtocolSpec>::Ack>,
    ) -> InboundDecision<<GetProtocol as ProtocolSpec>::Ack> {
        self.on_get_request(request, response)
    }
}

impl<H: NetworkEventHandler> ProtocolHandler<FinalityProtocol> for H {
    fn handle_request(
        &self,
        request: InboundRequest<<FinalityProtocol as ProtocolSpec>::RequestData>,
        response: ResponseHandle<<FinalityProtocol as ProtocolSpec>::Ack>,
    ) -> InboundDecision<<FinalityProtocol as ProtocolSpec>::Ack> {
        self.on_finality_request(request, response)
    }
}

impl<H: NetworkEventHandler> ProtocolHandler<BatchGetProtocol> for H {
    fn handle_request(
        &self,
        request: InboundRequest<<BatchGetProtocol as ProtocolSpec>::RequestData>,
        response: ResponseHandle<<BatchGetProtocol as ProtocolSpec>::Ack>,
    ) -> InboundDecision<<BatchGetProtocol as ProtocolSpec>::Ack> {
        self.on_batch_get_request(request, response)
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
    let message = RequestMessage::protocol_request(operation_id, request_data);
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
    observability::record_network_pending_requests(P::NAME, pending.len());
}

fn strip_p2p_protocol(addr: &Multiaddr) -> Multiaddr {
    addr.into_iter()
        .filter(|proto| !matches!(proto, Protocol::P2p(_)))
        .collect()
}

fn parse_peer_id_from_multiaddr(addr: &Multiaddr) -> Option<PeerId> {
    addr.iter().find_map(|proto| {
        if let Protocol::P2p(peer_id) = proto {
            Some(peer_id)
        } else {
            None
        }
    })
}

fn dial_peer_if_needed(swarm: &mut Swarm<NodeBehaviour>, peer: PeerId) {
    if *swarm.local_peer_id() == peer {
        tracing::trace!(%peer, "skipping self dial");
    } else if swarm.is_connected(&peer) {
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
pub struct NetworkEventLoop {
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
    bootstrap_peers: HashSet<PeerId>,
    kad_allowed_peers: HashSet<PeerId>,
}

impl NetworkEventLoop {
    fn refresh_kad_query_allowlist(&mut self) {
        self.swarm
            .behaviour_mut()
            .kad
            .set_query_peer_allowlist(self.kad_allowed_peers.iter().copied());
    }

    fn action_channel_fill_ratio(depth: usize) -> f64 {
        (depth as f64 / ACTION_CHANNEL_CAPACITY as f64).clamp(0.0, 1.0)
    }

    /// Create a new NetworkEventLoop.
    pub(super) fn new(
        swarm: Swarm<NodeBehaviour>,
        control_rx: mpsc::Receiver<NetworkControlAction>,
        data_rx: mpsc::Receiver<NetworkDataAction>,
        config: NetworkManagerConfig,
        peer_event_tx: broadcast::Sender<PeerEvent>,
        shutdown: CancellationToken,
    ) -> Self {
        observability::record_network_action_queue_depth("control", 0);
        observability::record_network_action_queue_depth("data", 0);
        observability::record_network_action_channel_fill("control", 0.0);
        observability::record_network_action_channel_fill("data", 0.0);
        observability::record_network_connected_peers(0);
        observability::record_network_pending_requests(StoreProtocol::NAME, 0);
        observability::record_network_pending_requests(GetProtocol::NAME, 0);
        observability::record_network_pending_requests(FinalityProtocol::NAME, 0);
        observability::record_network_pending_requests(BatchGetProtocol::NAME, 0);

        let mut bootstrap_peers: HashSet<PeerId> = config
            .bootstrap
            .iter()
            .filter_map(|addr| addr.parse::<Multiaddr>().ok())
            .filter_map(|addr| parse_peer_id_from_multiaddr(&addr))
            .collect();
        bootstrap_peers.remove(swarm.local_peer_id());
        let kad_allowed_peers = bootstrap_peers.clone();

        let mut this = Self {
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
            bootstrap_peers,
            kad_allowed_peers,
        };

        this.refresh_kad_query_allowlist();
        this
    }

    /// Starts listening on the configured port.
    ///
    /// # Errors
    /// Returns `NetworkError` if listener initialization fails.
    pub fn start_listening(&mut self) -> Result<(), NetworkError> {
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
    pub async fn run<H: NetworkEventHandler>(mut self, handler: &H) {
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
                    Ok(action) => {
                        observability::record_network_action_dequeue("control", action.kind());
                        self.handle_control_action(action);
                        let depth = self.control_rx.len();
                        observability::record_network_action_queue_depth("control", depth);
                        observability::record_network_action_channel_fill(
                            "control",
                            Self::action_channel_fill_ratio(depth),
                        );
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        tracing::info!("Control channel closed");
                        control_closed = true;
                        observability::record_network_action_queue_depth("control", 0);
                        observability::record_network_action_channel_fill("control", 0.0);
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
                        Some(action) => {
                            observability::record_network_action_dequeue("control", action.kind());
                            self.handle_control_action(action);
                            let depth = self.control_rx.len();
                            observability::record_network_action_queue_depth("control", depth);
                            observability::record_network_action_channel_fill(
                                "control",
                                Self::action_channel_fill_ratio(depth),
                            );
                        }
                        None => {
                            tracing::info!("Control channel closed");
                            control_closed = true;
                            observability::record_network_action_queue_depth("control", 0);
                            observability::record_network_action_channel_fill("control", 0.0);
                        }
                    }
                }
                data_action = self.data_rx.recv(), if !data_closed => {
                    match data_action {
                        Some(action) => {
                            observability::record_network_action_dequeue("data", action.kind());
                            self.handle_data_action(action);
                            let depth = self.data_rx.len();
                            observability::record_network_action_queue_depth("data", depth);
                            observability::record_network_action_channel_fill(
                                "data",
                                Self::action_channel_fill_ratio(depth),
                            );
                        }
                        None => {
                            tracing::info!("Data channel closed");
                            data_closed = true;
                            observability::record_network_action_queue_depth("data", 0);
                            observability::record_network_action_channel_fill("data", 0.0);
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
                    observability::record_network_peer_event("identify_received", "received");
                    let identify_info = IdentifyInfo {
                        protocols: info.protocols,
                        listen_addrs: info.listen_addrs,
                    };
                    if self.kad_allowed_peers.contains(&peer_id) {
                        for addr in identify_info.listen_addrs.iter() {
                            let addr = strip_p2p_protocol(addr);
                            self.swarm.behaviour_mut().kad.add_address(&peer_id, addr);
                        }
                    } else {
                        observability::record_network_peer_event(
                            "identify_received",
                            "ignored_non_allowed",
                        );
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
                                observability::record_network_peer_event("kad_lookup", "found");
                                let _ = self.peer_event_tx.send(PeerEvent::KadLookup {
                                    target,
                                    found: true,
                                });
                                if self.kad_allowed_peers.contains(&target) {
                                    dial_peer_if_needed(&mut self.swarm, target);
                                } else {
                                    observability::record_network_peer_event(
                                        "kad_lookup",
                                        "found_ignored_non_allowed",
                                    );
                                }
                            } else {
                                observability::record_network_peer_event("kad_lookup", "not_found");
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
                                observability::record_network_peer_event("kad_lookup", "found");
                                let _ = self.peer_event_tx.send(PeerEvent::KadLookup {
                                    target,
                                    found: true,
                                });
                                if self.kad_allowed_peers.contains(&target) {
                                    dial_peer_if_needed(&mut self.swarm, target);
                                } else {
                                    observability::record_network_peer_event(
                                        "kad_lookup",
                                        "found_ignored_non_allowed",
                                    );
                                }
                            } else {
                                observability::record_network_peer_event("kad_lookup", "not_found");
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
                observability::record_network_peer_event("connection_established", "established");
                observability::record_network_connected_peers(self.swarm.connected_peers().count());
                let _ = self
                    .peer_event_tx
                    .send(PeerEvent::ConnectionEstablished { peer_id });
            }
            SwarmEvent::ConnectionClosed { .. } => {
                observability::record_network_peer_event("connection_closed", "closed");
                observability::record_network_connected_peers(self.swarm.connected_peers().count());
            }
            _ => {}
        }
    }

    /// Handle a control-plane action.
    fn handle_control_action(&mut self, action: NetworkControlAction) {
        match action {
            NetworkControlAction::FindPeers(peers) => {
                let local_peer = *self.swarm.local_peer_id();
                let mut find_targets = Vec::new();
                for peer in peers {
                    if peer == local_peer {
                        continue;
                    }
                    self.kad_allowed_peers.insert(peer);
                    find_targets.push(peer);
                }
                self.refresh_kad_query_allowlist();
                for peer in find_targets {
                    self.swarm.behaviour_mut().kad.get_closest_peers(peer);
                }
            }
            NetworkControlAction::GetConnectedPeers(response_tx) => {
                let peers: Vec<PeerId> = self.swarm.connected_peers().copied().collect();
                let _ = response_tx.send(peers);
            }
            NetworkControlAction::AddAddresses { addresses } => {
                let local_peer = *self.swarm.local_peer_id();
                let mut total_addrs = 0usize;
                for (peer_id, addrs) in &addresses {
                    if *peer_id == local_peer {
                        continue;
                    }
                    self.kad_allowed_peers.insert(*peer_id);
                    for addr in addrs {
                        let addr = strip_p2p_protocol(addr);
                        self.swarm.behaviour_mut().kad.add_address(peer_id, addr);
                        total_addrs += 1;
                    }
                }
                self.refresh_kad_query_allowlist();
                tracing::info!(
                    peers = addresses.len(),
                    addresses = total_addrs,
                    "Added persisted peer addresses to Kademlia"
                );
            }
            NetworkControlAction::SyncKadAllowedPeers { peers } => {
                let local_peer = *self.swarm.local_peer_id();
                let mut target_allowed = self.bootstrap_peers.clone();
                target_allowed.extend(peers);
                target_allowed.remove(&local_peer);

                let to_remove: Vec<PeerId> = self
                    .kad_allowed_peers
                    .difference(&target_allowed)
                    .copied()
                    .collect();

                for peer_id in &to_remove {
                    self.kad_allowed_peers.remove(peer_id);
                    self.swarm.behaviour_mut().kad.remove_peer(peer_id);
                }

                let mut added = 0usize;
                for peer_id in target_allowed {
                    if self.kad_allowed_peers.insert(peer_id) {
                        added += 1;
                    }
                }
                self.refresh_kad_query_allowlist();

                tracing::debug!(
                    allowed = self.kad_allowed_peers.len(),
                    removed = to_remove.len(),
                    added,
                    "Reconciled Kademlia allowed peers"
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
                let status = if self
                    .swarm
                    .behaviour_mut()
                    .store
                    .send_response(channel, message)
                    .is_ok()
                {
                    "ok"
                } else {
                    "failed"
                };
                observability::record_network_response_send(StoreProtocol::NAME, "queued", status);
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
                let status = if self
                    .swarm
                    .behaviour_mut()
                    .get
                    .send_response(channel, message)
                    .is_ok()
                {
                    "ok"
                } else {
                    "failed"
                };
                observability::record_network_response_send(GetProtocol::NAME, "queued", status);
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
                let status = if self
                    .swarm
                    .behaviour_mut()
                    .finality
                    .send_response(channel, message)
                    .is_ok()
                {
                    "ok"
                } else {
                    "failed"
                };
                observability::record_network_response_send(
                    FinalityProtocol::NAME,
                    "queued",
                    status,
                );
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
                let status = if self
                    .swarm
                    .behaviour_mut()
                    .batch_get
                    .send_response(channel, message)
                    .is_ok()
                {
                    "ok"
                } else {
                    "failed"
                };
                observability::record_network_response_send(
                    BatchGetProtocol::NAME,
                    "queued",
                    status,
                );
            }
        }
    }
}
