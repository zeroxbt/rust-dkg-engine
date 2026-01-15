use std::sync::Arc;

use futures::stream::{FuturesUnordered, StreamExt};
use libp2p::request_response::Message;
use network::{NestedBehaviourEvent, NetworkManager, SwarmEvent, identify, request_response};
use repository::RepositoryManager;
use tokio::sync::{Semaphore, mpsc};

use super::constants::NETWORK_EVENT_QUEUE_PARALLELISM;
use crate::{
    context::Context,
    controllers::rpc_controller::store_controller::StoreController,
    network::{NetworkProtocols, NetworkProtocolsEvent},
};

// Type alias for the complete behaviour and its event type
type Behaviour = network::NestedBehaviour<NetworkProtocols>;
type BehaviourEvent = <Behaviour as network::NetworkBehaviour>::ToSwarm;

pub struct RpcRouter {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    store_controller: Arc<StoreController>,
    semaphore: Arc<Semaphore>,
}

impl RpcRouter {
    pub fn new(context: Arc<Context>) -> Self {
        RpcRouter {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            store_controller: Arc::new(StoreController::new(Arc::clone(&context))),
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
                                Message::Response { response, .. } => {
                                    self.store_controller.handle_response(response, peer).await;
                                }
                            };
                        }
                        x => {
                            println!("{:?}", x)
                        }
                    },
                    NetworkProtocolsEvent::Get(inner_event) => match inner_event {
                        request_response::Event::OutboundFailure { error, .. } => {
                            tracing::error!("Failed to get: {}", error)
                        }
                        request_response::Event::Message { .. } => {
                            // self.get_controller.handle_message(message, peer).await;
                        }
                        _ => {}
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
