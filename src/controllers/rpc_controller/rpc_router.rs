use std::sync::Arc;

use futures::stream::{FuturesUnordered, StreamExt};
use libp2p::request_response::Message;
use tokio::sync::{Semaphore, mpsc};

use super::{
    constants::NETWORK_EVENT_QUEUE_PARALLELISM,
    protocols::{NetworkProtocols, NetworkProtocolsEvent},
};
use crate::{
    context::Context,
    controllers::rpc_controller::v1::{
        batch_get_rpc_controller::BatchGetRpcController,
        finality_rpc_controller::FinalityRpcController, get_rpc_controller::GetRpcController,
        store_rpc_controller::StoreRpcController,
    },
    managers::network::{
        CompositeBehaviour, CompositeBehaviourEvent, NetworkBehaviour, NetworkManager, SwarmEvent,
        identify, kad, request_response,
    },
    services::{PeerDiscoveryTracker, RequestError},
};

// Type alias for the complete behaviour and its event type
type Behaviour = CompositeBehaviour<NetworkProtocols>;
type BehaviourEvent = <Behaviour as NetworkBehaviour>::ToSwarm;

pub(crate) struct RpcRouter {
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
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
                CompositeBehaviourEvent::Protocols(protocol_event) => match protocol_event {
                    NetworkProtocolsEvent::Store(inner_event) => match inner_event {
                        request_response::Event::Message { message, peer, .. } => {
                            match message {
                                Message::Request {
                                    request, channel, ..
                                } => {
                                    self.store_controller
                                        .handle_request(request, channel, peer)
                                        .await;
                                }
                                Message::Response {
                                    response,
                                    request_id,
                                } => {
                                    // Complete pending request for send_request callers
                                    self.store_controller
                                        .operation_service()
                                        .pending_requests()
                                        .complete_success(request_id, response.data);
                                }
                            };
                        }
                        // OutboundFailure: We sent a store request and it failed (timeout,
                        // connection closed, etc.)
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
                                "Store request failed"
                            );

                            // Complete pending request with failure for send_request callers
                            let request_error = match &error {
                                request_response::OutboundFailure::Timeout => RequestError::Timeout,
                                request_response::OutboundFailure::ConnectionClosed => {
                                    RequestError::ConnectionFailed("Connection closed".to_string())
                                }
                                request_response::OutboundFailure::DialFailure => {
                                    RequestError::NotConnected
                                }
                                request_response::OutboundFailure::UnsupportedProtocols => {
                                    RequestError::ConnectionFailed(
                                        "Unsupported protocols".to_string(),
                                    )
                                }
                                _ => RequestError::ConnectionFailed(error.to_string()),
                            };
                            self.store_controller
                                .operation_service()
                                .pending_requests()
                                .complete_failure(request_id, request_error);
                        }
                        // InboundFailure: Someone sent us a store request and we failed to respond.
                        // This is informational - the requester will see OutboundFailure on their
                        // end. Common causes:
                        //   - Timeout: Our command handler took too long to call send_response
                        //   - ResponseOmission: SessionManager channel was dropped without
                        //     responding
                        //   - ConnectionClosed: Requester disconnected before we could respond
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
                                "Failed to respond to store request from peer"
                            );
                        }
                        // ResponseSent: Confirms our response was successfully sent.
                        // Useful for debugging/metrics but no action needed.
                        request_response::Event::ResponseSent {
                            peer, request_id, ..
                        } => {
                            tracing::trace!(
                                %peer,
                                ?request_id,
                                "Store response sent successfully"
                            );
                        }
                    },
                    NetworkProtocolsEvent::Get(inner_event) => match inner_event {
                        request_response::Event::Message { message, peer, .. } => {
                            match message {
                                Message::Request {
                                    request, channel, ..
                                } => {
                                    self.get_controller
                                        .handle_request(request, channel, peer)
                                        .await;
                                }
                                Message::Response {
                                    response,
                                    request_id,
                                } => {
                                    // Complete pending request for send_request callers
                                    self.get_controller
                                        .operation_service()
                                        .pending_requests()
                                        .complete_success(request_id, response.data);
                                }
                            };
                        }
                        // OutboundFailure: We sent a get request and it failed (timeout,
                        // connection closed, etc.)
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
                                "Get request failed"
                            );

                            // Complete pending request with failure for send_request callers
                            let request_error = match &error {
                                request_response::OutboundFailure::Timeout => RequestError::Timeout,
                                request_response::OutboundFailure::ConnectionClosed => {
                                    RequestError::ConnectionFailed("Connection closed".to_string())
                                }
                                request_response::OutboundFailure::DialFailure => {
                                    RequestError::NotConnected
                                }
                                request_response::OutboundFailure::UnsupportedProtocols => {
                                    RequestError::ConnectionFailed(
                                        "Unsupported protocols".to_string(),
                                    )
                                }
                                _ => RequestError::ConnectionFailed(error.to_string()),
                            };
                            self.get_controller
                                .operation_service()
                                .pending_requests()
                                .complete_failure(request_id, request_error);
                        }
                        // InboundFailure: Someone sent us a get request and we failed to respond.
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
                                "Failed to respond to get request from peer"
                            );
                        }
                        request_response::Event::ResponseSent {
                            peer, request_id, ..
                        } => {
                            tracing::trace!(
                                %peer,
                                ?request_id,
                                "Get response sent successfully"
                            );
                        }
                    },
                    NetworkProtocolsEvent::Finality(inner_event) => match inner_event {
                        request_response::Event::Message { message, peer, .. } => {
                            match message {
                                Message::Request {
                                    request, channel, ..
                                } => {
                                    self.finality_controller
                                        .handle_request(request, channel, peer)
                                        .await;
                                }
                                // TODO: Track request_id to detect late responses after timeout.
                                Message::Response { response, .. } => {
                                    self.finality_controller
                                        .handle_response(response, peer)
                                        .await;
                                }
                            };
                        }
                        // OutboundFailure: We sent a finality request and it failed.
                        // Note: Finality is typically a notification, so failure handling
                        // may differ from store protocol (no replication counting).
                        // TODO: Decide if finality failures need specific handling.
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
                                "Finality request failed"
                            );
                        }
                        // InboundFailure: Someone sent us a finality request and we failed to
                        // respond.
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
                                "Failed to respond to finality request from peer"
                            );
                        }
                        request_response::Event::ResponseSent {
                            peer, request_id, ..
                        } => {
                            tracing::trace!(
                                %peer,
                                ?request_id,
                                "Finality response sent successfully"
                            );
                        }
                    },
                    NetworkProtocolsEvent::BatchGet(inner_event) => match inner_event {
                        request_response::Event::Message { message, peer, .. } => {
                            match message {
                                Message::Request {
                                    request, channel, ..
                                } => {
                                    self.batch_get_controller
                                        .handle_request(request, channel, peer)
                                        .await;
                                }
                                Message::Response {
                                    response,
                                    request_id,
                                } => {
                                    // Complete pending request for send_request callers
                                    self.batch_get_controller
                                        .operation_service()
                                        .pending_requests()
                                        .complete_success(request_id, response.data);
                                }
                            };
                        }
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
                                "Batch get request failed"
                            );

                            // Complete pending request with failure for send_request callers
                            let request_error = match &error {
                                request_response::OutboundFailure::Timeout => RequestError::Timeout,
                                request_response::OutboundFailure::ConnectionClosed => {
                                    RequestError::ConnectionFailed("Connection closed".to_string())
                                }
                                request_response::OutboundFailure::DialFailure => {
                                    RequestError::NotConnected
                                }
                                request_response::OutboundFailure::UnsupportedProtocols => {
                                    RequestError::ConnectionFailed(
                                        "Unsupported protocols".to_string(),
                                    )
                                }
                                _ => RequestError::ConnectionFailed(error.to_string()),
                            };
                            self.batch_get_controller
                                .operation_service()
                                .pending_requests()
                                .complete_failure(request_id, request_error);
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
                                "Failed to respond to batch get request from peer"
                            );
                        }
                        request_response::Event::ResponseSent {
                            peer, request_id, ..
                        } => {
                            tracing::trace!(
                                %peer,
                                ?request_id,
                                "Batch get response sent successfully"
                            );
                        }
                    },
                },
                CompositeBehaviourEvent::Identify(identify::Event::Received {
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
                CompositeBehaviourEvent::Kad(kad::Event::OutboundQueryProgressed {
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
