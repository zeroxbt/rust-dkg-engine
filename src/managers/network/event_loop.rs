//! NetworkEventLoop - owns the swarm and runs the event loop.
//!
//! This is the internal implementation that handles all network events.
//! It receives actions from NetworkManager handles and processes swarm events.

use libp2p::{Multiaddr, PeerId, Swarm, identify, kad, request_response, swarm::SwarmEvent};
use libp2p::swarm::NetworkBehaviour;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use super::{
    JsCompatCodec, NetworkError, NetworkEventHandler, NetworkManagerConfig, PendingRequests,
    RequestMessage, ResponseMessage,
    actions::NetworkAction,
    behaviour::{NodeBehaviour, NodeBehaviourEvent},
    message::{RequestMessageHeader, RequestMessageType},
    protocols::{
        BatchGetProtocol, BatchGetResponseData, FinalityProtocol, FinalityResponseData,
        GetProtocol, GetResponseData, ProtocolResponse, ProtocolSpec, StoreProtocol,
        StoreResponseData,
    },
};

/// Handle a protocol event - processes responses and dispatches requests.
async fn handle_protocol_event<P, H>(
    event: request_response::Event<RequestMessage<P::RequestData>, ResponseMessage<P::Ack>>,
    pending: &mut PendingRequests<ProtocolResponse<P>>,
    handler: &H,
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
                pending.complete_success(request_id, response.data);
            }
            request_response::Message::Request {
                request, channel, ..
            } => {
                handler.handle_request(request, channel, peer).await;
            }
        },
        request_response::Event::OutboundFailure {
            request_id,
            error,
            peer,
            ..
        } => {
            tracing::warn!(%peer, ?request_id, ?error, "{} request failed", P::NAME);
            pending.complete_failure(request_id, NetworkError::from(&error));
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
    ) -> impl std::future::Future<Output = ()> + Send;
}

impl<H: NetworkEventHandler> ProtocolHandler<StoreProtocol> for H {
    async fn handle_request(
        &self,
        request: RequestMessage<<StoreProtocol as ProtocolSpec>::RequestData>,
        channel: request_response::ResponseChannel<
            ResponseMessage<<StoreProtocol as ProtocolSpec>::Ack>,
        >,
        peer: PeerId,
    ) {
        self.on_store_request(request, channel, peer).await;
    }
}

impl<H: NetworkEventHandler> ProtocolHandler<GetProtocol> for H {
    async fn handle_request(
        &self,
        request: RequestMessage<<GetProtocol as ProtocolSpec>::RequestData>,
        channel: request_response::ResponseChannel<
            ResponseMessage<<GetProtocol as ProtocolSpec>::Ack>,
        >,
        peer: PeerId,
    ) {
        self.on_get_request(request, channel, peer).await;
    }
}

impl<H: NetworkEventHandler> ProtocolHandler<FinalityProtocol> for H {
    async fn handle_request(
        &self,
        request: RequestMessage<<FinalityProtocol as ProtocolSpec>::RequestData>,
        channel: request_response::ResponseChannel<
            ResponseMessage<<FinalityProtocol as ProtocolSpec>::Ack>,
        >,
        peer: PeerId,
    ) {
        self.on_finality_request(request, channel, peer).await;
    }
}

impl<H: NetworkEventHandler> ProtocolHandler<BatchGetProtocol> for H {
    async fn handle_request(
        &self,
        request: RequestMessage<<BatchGetProtocol as ProtocolSpec>::RequestData>,
        channel: request_response::ResponseChannel<
            ResponseMessage<<BatchGetProtocol as ProtocolSpec>::Ack>,
        >,
        peer: PeerId,
    ) {
        self.on_batch_get_request(request, channel, peer).await;
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
    let request_id = behaviour_accessor(swarm.behaviour_mut()).send_request(&peer, message);
    pending.insert_sender(request_id, response_tx);
}

/// NetworkEventLoop owns the swarm and runs the event loop.
///
/// This is created by `NetworkManager::connect()` and runs in its own task.
pub(crate) struct NetworkEventLoop {
    swarm: Swarm<NodeBehaviour>,
    action_rx: mpsc::Receiver<NetworkAction>,
    config: NetworkManagerConfig,
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
        action_rx: mpsc::Receiver<NetworkAction>,
        config: NetworkManagerConfig,
    ) -> Self {
        Self {
            swarm,
            action_rx,
            config,
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

    /// Runs the network event loop, processing swarm events and actions.
    ///
    /// This method consumes self and runs until the action channel is closed.
    pub(crate) async fn run<H: NetworkEventHandler>(mut self, handler: &H) {
        use libp2p::futures::StreamExt;

        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event, handler).await;
                }
                action = self.action_rx.recv() => {
                    match action {
                        Some(action) => self.handle_action(action),
                        None => {
                            tracing::info!("Action channel closed, shutting down network loop");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Process a swarm event.
    async fn handle_swarm_event<H: NetworkEventHandler>(
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
                        &mut self.pending_store,
                        handler,
                    )
                    .await;
                }
                NodeBehaviourEvent::Get(inner) => {
                    handle_protocol_event::<GetProtocol, _>(
                        inner,
                        &mut self.pending_get,
                        handler,
                    )
                    .await;
                }
                NodeBehaviourEvent::Finality(inner) => {
                    handle_protocol_event::<FinalityProtocol, _>(
                        inner,
                        &mut self.pending_finality,
                        handler,
                    )
                    .await;
                }
                NodeBehaviourEvent::BatchGet(inner) => {
                    handle_protocol_event::<BatchGetProtocol, _>(
                        inner,
                        &mut self.pending_batch_get,
                        handler,
                    )
                    .await;
                }

                // Identify protocol
                NodeBehaviourEvent::Identify(identify::Event::Received {
                    peer_id, info, ..
                }) => {
                    handler
                        .on_identify_received(peer_id, info.listen_addrs)
                        .await;
                }

                // Kademlia protocol
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

    /// Handle a network action.
    fn handle_action(&mut self, action: NetworkAction) {
        match action {
            NetworkAction::FindPeers(peers) => {
                for peer in peers {
                    self.swarm.behaviour_mut().kad.get_closest_peers(peer);
                }
            }
            NetworkAction::DialPeer(peer) => {
                if self.swarm.is_connected(&peer) {
                    tracing::trace!(%peer, "already connected to peer, skipping dial");
                } else if let Err(e) = self.swarm.dial(peer) {
                    tracing::debug!(%peer, error = %e, "failed to dial peer");
                }
            }
            NetworkAction::GetConnectedPeers(response_tx) => {
                let peers: Vec<PeerId> = self.swarm.connected_peers().copied().collect();
                let _ = response_tx.send(peers);
            }
            NetworkAction::GetPeerAddresses {
                peer_id,
                response_tx,
            } => {
                let addresses = self
                    .swarm
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
                    self.swarm.behaviour_mut().kad.add_address(&peer_id, address);
                }
            }
            // Protocol request/response actions
            NetworkAction::SendStoreRequest {
                peer,
                addresses: _,
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
            NetworkAction::SendStoreResponse { channel, message } => {
                let _ = self
                    .swarm
                    .behaviour_mut()
                    .store
                    .send_response(channel, message);
            }
            NetworkAction::SendGetRequest {
                peer,
                addresses: _,
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
            NetworkAction::SendGetResponse { channel, message } => {
                let _ = self
                    .swarm
                    .behaviour_mut()
                    .get
                    .send_response(channel, message);
            }
            NetworkAction::SendFinalityRequest {
                peer,
                addresses: _,
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
            NetworkAction::SendFinalityResponse { channel, message } => {
                let _ = self
                    .swarm
                    .behaviour_mut()
                    .finality
                    .send_response(channel, message);
            }
            NetworkAction::SendBatchGetRequest {
                peer,
                addresses: _,
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
            NetworkAction::SendBatchGetResponse { channel, message } => {
                let _ = self
                    .swarm
                    .behaviour_mut()
                    .batch_get
                    .send_response(channel, message);
            }
        }
    }
}
