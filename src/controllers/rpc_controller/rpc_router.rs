use std::sync::Arc;

use futures::stream::{FuturesUnordered, StreamExt};
use network::{
    BehaviourEvent, NetworkEvent, SwarmEvent, action::NetworkAction, identify, request_response,
};
use repository::RepositoryManager;
use tokio::sync::{
    Semaphore,
    mpsc::{self, Receiver},
};
use tracing::{error, info};

use super::constants::NETWORK_EVENT_QUEUE_PARALLELISM;
use crate::{
    context::Context,
    controllers::rpc_controller::{
        get_controller::GetController, store_controller::StoreController,
    },
    types::traits::controller::BaseController,
};

pub struct RpcRouter {
    repository_manager: Arc<RepositoryManager>,
    network_action_tx: mpsc::Sender<NetworkAction>,
    get_controller: Arc<GetController>,
    store_controller: Arc<StoreController>,
    pub semaphore: Arc<Semaphore>,
}

impl RpcRouter {
    pub fn new(context: Arc<Context>) -> Self {
        RpcRouter {
            repository_manager: Arc::clone(context.repository_manager()),
            network_action_tx: context.network_action_tx().clone(),
            get_controller: Arc::new(GetController::new(Arc::clone(&context))),
            store_controller: Arc::new(StoreController::new(Arc::clone(&context))),
            semaphore: Arc::new(Semaphore::new(NETWORK_EVENT_QUEUE_PARALLELISM)),
        }
    }

    pub async fn listen_and_handle_network_events(
        &self,
        mut network_event_rx: Receiver<NetworkEvent>,
    ) {
        let mut pending_tasks = FuturesUnordered::new();

        loop {
            tokio::select! {
                _ = pending_tasks.select_next_some(), if !pending_tasks.is_empty() => {
                    // Continue the loop when a task completes.
                }
                event = network_event_rx.recv(), if self.semaphore.available_permits() > 0 => {
                    if let Some(event) = event {
                        pending_tasks.push(self.handle_network_event(event));
                    }
                }
            }
        }
    }

    async fn handle_network_event(&self, event: NetworkEvent) {
        let _permit = self.semaphore.acquire().await.unwrap();

        match event {
            SwarmEvent::Behaviour(BehaviourEvent::Store(inner_event)) => match inner_event {
                request_response::Event::OutboundFailure { error, .. } => {
                    error!("Failed to store: {}", error);
                }
                request_response::Event::Message {
                    message,
                    peer,
                    connection_id,
                } => {
                    self.store_controller.handle_message(message, peer).await;
                }
                _ => {}
            },
            SwarmEvent::Behaviour(BehaviourEvent::Get(inner_event)) => match inner_event {
                request_response::Event::OutboundFailure { error, .. } => {
                    error!("Failed to get: {}", error)
                }
                request_response::Event::Message {
                    peer,
                    message,
                    connection_id,
                } => {
                    self.get_controller.handle_message(message, peer).await;
                }
                _ => {}
            },
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Listening on {}", address)
            }
            SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                info,
                connection_id,
            })) => {
                tracing::trace!("Adding peer to routing table: {}", peer_id);

                self.network_action_tx
                    .send(NetworkAction::AddAddress {
                        peer_id,
                        addresses: info.listen_addrs,
                    })
                    .await
                    .unwrap();

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
        }
    }
}
