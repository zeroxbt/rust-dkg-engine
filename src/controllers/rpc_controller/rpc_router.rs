use std::sync::Arc;

use futures::stream::{FuturesUnordered, StreamExt};
use libp2p::request_response::Message;
use tokio::sync::{Semaphore, mpsc};

use super::constants::NETWORK_EVENT_QUEUE_PARALLELISM;
use crate::{
    context::Context,
    controllers::rpc_controller::v1::{
        batch_get_rpc_controller::BatchGetRpcController,
        finality_rpc_controller::FinalityRpcController, get_rpc_controller::GetRpcController,
        store_rpc_controller::StoreRpcController,
    },
    managers::network::{
        NetworkBehaviour, NetworkManager, NodeBehaviour, NodeBehaviourEvent, RequestError,
        SwarmEvent, identify, kad, request_response,
    },
    services::PeerDiscoveryTracker,
};

/// Macro to handle protocol events with consistent patterns across all protocols.
/// Handles Message (Request/Response), OutboundFailure, InboundFailure, and ResponseSent events.
macro_rules! handle_protocol_event {
    (
        $inner_event:expr,
        $controller:expr,
        $pending:expr,
        $protocol_name:literal
    ) => {
        match $inner_event {
            request_response::Event::Message { message, peer, .. } => match message {
                Message::Request {
                    request, channel, ..
                } => {
                    $controller.handle_request(request, channel, peer).await;
                }
                Message::Response {
                    response,
                    request_id,
                } => {
                    $pending.complete_success(request_id, response.data);
                }
            },
            request_response::Event::OutboundFailure {
                peer,
                request_id,
                error,
                ..
            } => {
                tracing::warn!(
                    %peer,
                    ?request_id,
                    ?error,
                    concat!($protocol_name, " request failed")
                );
                $pending.complete_failure(request_id, RequestError::from(&error));
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
                    concat!("Failed to respond to ", $protocol_name, " request from peer")
                );
            }
            request_response::Event::ResponseSent {
                peer, request_id, ..
            } => {
                tracing::trace!(
                    %peer,
                    ?request_id,
                    concat!($protocol_name, " response sent successfully")
                );
            }
        }
    };
}

// Type alias for the complete behaviour and its event type
type BehaviourEvent = <NodeBehaviour as NetworkBehaviour>::ToSwarm;

pub(crate) struct RpcRouter {
    network_manager: Arc<NetworkManager>,
    store_controller: Arc<StoreRpcController>,
    get_controller: Arc<GetRpcController>,
    finality_controller: Arc<FinalityRpcController>,
    batch_get_controller: Arc<BatchGetRpcController>,
    peer_discovery_tracker: Arc<PeerDiscoveryTracker>,
    semaphore: Arc<Semaphore>,
}

impl RpcRouter {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        RpcRouter {
            network_manager: Arc::clone(context.network_manager()),
            store_controller: Arc::new(StoreRpcController::new(Arc::clone(&context))),
            get_controller: Arc::new(GetRpcController::new(Arc::clone(&context))),
            finality_controller: Arc::new(FinalityRpcController::new(Arc::clone(&context))),
            batch_get_controller: Arc::new(BatchGetRpcController::new(Arc::clone(&context))),
            peer_discovery_tracker: Arc::clone(context.peer_discovery_tracker()),
            semaphore: Arc::new(Semaphore::new(NETWORK_EVENT_QUEUE_PARALLELISM)),
        }
    }

    pub(crate) async fn listen_and_handle_network_events(
        &self,
        mut event_rx: mpsc::Receiver<SwarmEvent<BehaviourEvent>>,
    ) {
        let mut pending_tasks = FuturesUnordered::new();

        loop {
            tokio::select! {
                _ = pending_tasks.select_next_some(), if !pending_tasks.is_empty() => {
                    // Continue the loop when a task completes.
                }
                event = event_rx.recv() => {
                    match event {
                        Some(event) => {
                            let permit = self.semaphore.clone().acquire_owned().await.expect("Acquire permit");
                            pending_tasks.push(self.handle_network_event(event, permit));
                        }
                        None => {
                            tracing::error!("Network event channel closed, shutting down RPC router");
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn handle_network_event(
        &self,
        event: SwarmEvent<BehaviourEvent>,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        match event {
            SwarmEvent::Behaviour(behaviour_event) => match behaviour_event {
                // Application protocol events (flattened - no more nested Protocols wrapper)
                NodeBehaviourEvent::Store(inner_event) => {
                    handle_protocol_event!(
                        inner_event,
                        self.store_controller,
                        self.network_manager.pending_store(),
                        "Store"
                    );
                }
                NodeBehaviourEvent::Get(inner_event) => {
                    handle_protocol_event!(
                        inner_event,
                        self.get_controller,
                        self.network_manager.pending_get(),
                        "Get"
                    );
                }
                NodeBehaviourEvent::Finality(inner_event) => {
                    handle_protocol_event!(
                        inner_event,
                        self.finality_controller,
                        self.network_manager.pending_finality(),
                        "Finality"
                    );
                }
                NodeBehaviourEvent::BatchGet(inner_event) => {
                    handle_protocol_event!(
                        inner_event,
                        self.batch_get_controller,
                        self.network_manager.pending_batch_get(),
                        "Batch get"
                    );
                }
                // Infrastructure protocol events
                NodeBehaviourEvent::Identify(identify::Event::Received {
                    peer_id,
                    info,
                    ..
                }) => {
                    // tracing::debug!(%peer_id, "received identify from peer");

                    if let Err(error) = self
                        .network_manager
                        .add_kad_addresses(peer_id, info.listen_addrs)
                        .await
                    {
                        tracing::error!(%peer_id, %error, "failed to enqueue kad addresses");
                    }
                }
                NodeBehaviourEvent::Kad(kad::Event::OutboundQueryProgressed {
                    result: kad::QueryResult::GetClosestPeers(result),
                    ..
                }) => {
                    // Handle completed GetClosestPeers queries (from find_peers)
                    match result {
                        Ok(kad::GetClosestPeersOk { key, peers }) => {
                            let Ok(target_peer_id) = libp2p::PeerId::from_bytes(&key) else {
                                return;
                            };

                            // Check if our target peer is among the closest peers found
                            if peers.iter().any(|p| p.peer_id == target_peer_id) {
                                tracing::debug!(%target_peer_id, "DHT lookup found target peer, dialing");
                                if let Err(e) = self.network_manager.dial_peer(target_peer_id).await
                                {
                                    tracing::debug!(%target_peer_id, error = %e, "failed to dial peer");
                                }
                            } else {
                                tracing::debug!(
                                    %target_peer_id,
                                    peer_count = peers.len(),
                                    "DHT lookup did not find target peer"
                                );
                                // Record failure for backoff
                                self.peer_discovery_tracker.record_failure(target_peer_id);
                            }
                        }
                        Err(kad::GetClosestPeersError::Timeout { key, peers }) => {
                            let Ok(target_peer_id) = libp2p::PeerId::from_bytes(&key) else {
                                return;
                            };

                            // Even on timeout, check if target was among partial results
                            if peers.iter().any(|p| p.peer_id == target_peer_id) {
                                tracing::debug!(%target_peer_id, "DHT lookup timed out but found target peer, dialing");
                                if let Err(e) = self.network_manager.dial_peer(target_peer_id).await
                                {
                                    tracing::debug!(%target_peer_id, error = %e, "failed to dial peer");
                                }
                            } else {
                                tracing::debug!(
                                    %target_peer_id,
                                    peers_found = peers.len(),
                                    "DHT lookup timed out without finding target"
                                );
                                // Record failure for backoff
                                self.peer_discovery_tracker.record_failure(target_peer_id);
                            }
                        }
                    }
                }
                _ => {}
            },
            SwarmEvent::NewListenAddr { address, .. } => {
                tracing::info!("Listening on {}", address)
            }
            SwarmEvent::OutgoingConnectionError {
                peer_id: Some(peer_id),
                error,
                ..
            } => {
                // tracing::debug!(%peer_id, %error, "outgoing connection failed");
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                // tracing::debug!(%peer_id, "connection established");
                // Clear backoff on successful connection
                self.peer_discovery_tracker.record_success(&peer_id);
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established,
                ..
            } => {
                // tracing::debug!(%peer_id, num_established, "connection closed");
            }
            _ => {}
        }
    }
}
