use std::sync::Arc;

use futures::stream::{FuturesUnordered, StreamExt};
use libp2p::request_response::Message;
use network::{NestedBehaviourEvent, NetworkManager, SwarmEvent, identify, request_response};
use repository::RepositoryManager;
use tokio::sync::{Semaphore, mpsc};

use super::{
    constants::NETWORK_EVENT_QUEUE_PARALLELISM,
    protocols::{NetworkProtocols, NetworkProtocolsEvent},
};
use crate::{
    context::Context,
    controllers::rpc_controller::v1::{
        finality_rpc_controller::FinalityRpcController, get_rpc_controller::GetRpcController,
        store_rpc_controller::StoreRpcController,
    },
};

// Type alias for the complete behaviour and its event type
type Behaviour = network::NestedBehaviour<NetworkProtocols>;
type BehaviourEvent = <Behaviour as network::NetworkBehaviour>::ToSwarm;

pub struct RpcRouter {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    store_controller: Arc<StoreRpcController>,
    get_controller: Arc<GetRpcController>,
    finality_controller: Arc<FinalityRpcController>,
    semaphore: Arc<Semaphore>,
}

impl RpcRouter {
    pub fn new(context: Arc<Context>) -> Self {
        RpcRouter {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            store_controller: Arc::new(StoreRpcController::new(Arc::clone(&context))),
            get_controller: Arc::new(GetRpcController::new(Arc::clone(&context))),
            finality_controller: Arc::new(FinalityRpcController::new(Arc::clone(&context))),
            semaphore: Arc::new(Semaphore::new(NETWORK_EVENT_QUEUE_PARALLELISM)),
        }
    }

    pub async fn listen_and_handle_network_events(
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
                            let permit = self.semaphore.clone().acquire_owned().await.unwrap();
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
                NestedBehaviourEvent::Protocols(protocol_event) => match protocol_event {
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
                                    // Check if this response arrived after a timeout was already
                                    // processed
                                    if self
                                        .store_controller
                                        .operation_service()
                                        .request_tracker()
                                        .handle_response(request_id)
                                        .is_some()
                                    {
                                        // Valid response - process it
                                        self.store_controller.handle_response(response, peer).await;
                                    } else {
                                        // Late response after timeout, or unknown request
                                        tracing::debug!(
                                            %peer,
                                            ?request_id,
                                            "Ignoring late store response (already timed out or unknown)"
                                        );
                                    }
                                }
                            };
                        }
                        // OutboundFailure: We sent a store request and it failed (timeout,
                        // connection closed, etc.) This is treated as a
                        // NACK for replication counting.
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

                            let operation_service = self.store_controller.operation_service();

                            // Look up operation_id and mark as timed out to ignore late responses
                            if let Some((operation_id, _peer)) = operation_service
                                .request_tracker()
                                .handle_timeout(request_id)
                            {
                                // Record as NACK (failed response)
                                if let Err(e) =
                                    operation_service.record_response(operation_id, false).await
                                {
                                    tracing::error!(
                                        %operation_id,
                                        error = %e,
                                        "Failed to record timeout as NACK"
                                    );
                                }
                            } else {
                                tracing::debug!(
                                    ?request_id,
                                    "OutboundFailure for unknown request_id (not tracked)"
                                );
                            }
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
                                    // Check if this response arrived after a timeout was already
                                    // processed
                                    if self
                                        .get_controller
                                        .operation_service()
                                        .request_tracker()
                                        .handle_response(request_id)
                                        .is_some()
                                    {
                                        // Valid response - process it
                                        self.get_controller.handle_response(response, peer).await;
                                    } else {
                                        // Late response after timeout, or unknown request
                                        tracing::debug!(
                                            %peer,
                                            ?request_id,
                                            "Ignoring late get response (already timed out or unknown)"
                                        );
                                    }
                                }
                            };
                        }
                        // OutboundFailure: We sent a get request and it failed (timeout,
                        // connection closed, etc.) This is treated as a NACK.
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

                            let operation_service = self.get_controller.operation_service();

                            // Look up operation_id and mark as timed out to ignore late responses
                            if let Some((operation_id, _peer)) = operation_service
                                .request_tracker()
                                .handle_timeout(request_id)
                            {
                                // Record as NACK (failed response)
                                if let Err(e) =
                                    operation_service.record_response(operation_id, false).await
                                {
                                    tracing::error!(
                                        %operation_id,
                                        error = %e,
                                        "Failed to record get timeout as NACK"
                                    );
                                }
                            } else {
                                tracing::debug!(
                                    ?request_id,
                                    "Get OutboundFailure for unknown request_id (not tracked)"
                                );
                            }
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
                },
                NestedBehaviourEvent::Identify(identify::Event::Received {
                    peer_id, info, ..
                }) => {
                    tracing::trace!("Adding peer to routing table: {}", peer_id);

                    if let Err(error) = self
                        .network_manager
                        .add_kad_addresses(peer_id, info.listen_addrs)
                        .await
                    {
                        tracing::error!("Failed to enqueue kad addresses: {}", error);
                    }

                    self.repository_manager
                        .shard_repository()
                        .update_peer_record_last_seen_and_last_dialed(
                            peer_id.to_base58(),
                            chrono::Utc::now(),
                        )
                        .await
                        .unwrap();
                }
                _ => {}
            },
            SwarmEvent::NewListenAddr { address, .. } => {
                tracing::info!("Listening on {}", address)
            }
            _ => {}
        }
    }
}
