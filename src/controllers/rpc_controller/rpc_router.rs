use std::sync::Arc;

use futures::stream::{FuturesUnordered, StreamExt};
use network::{NetworkManager, SwarmEvent, identify, request_response};
use repository::RepositoryManager;
use tokio::sync::{Semaphore, mpsc};
use tracing::{error, info};

use super::constants::NETWORK_EVENT_QUEUE_PARALLELISM;
use crate::{
    context::Context,
    controllers::rpc_controller::{
        get_controller::GetController, store_controller::StoreController,
    },
    network::{NetworkHandle, NetworkProtocols},
    types::traits::controller::BaseController,
};

// Type alias for the complete behaviour and its event type
type Behaviour = network::NestedBehaviour<NetworkProtocols>;
type BehaviourEvent = <Behaviour as network::NetworkBehaviour>::ToSwarm;

pub struct RpcRouter {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    network_handle: Arc<NetworkHandle>,
    get_controller: Arc<GetController>,
    store_controller: Arc<StoreController>,
    pub semaphore: Arc<Semaphore>,
}

impl RpcRouter {
    pub fn new(context: Arc<Context>) -> Self {
        RpcRouter {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            network_handle: Arc::clone(context.network_handle()),
            get_controller: Arc::new(GetController::new(Arc::clone(&context))),
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
                        Some(event) if self.semaphore.available_permits() > 0 => {
                            pending_tasks.push(self.handle_network_event(event));
                        }
                        Some(_) => {
                            // No permits available, skip this event (or could buffer it)
                            // This maintains backpressure
                        }
                        None => {
                            error!("Network event channel closed, shutting down RPC router");
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn handle_network_event(&self, event: SwarmEvent<BehaviourEvent>) {
        let _permit = self.semaphore.acquire().await.unwrap();

        // We need to import the generated event enum types
        // NestedBehaviour<NetworkProtocols> generates NestedBehaviourEvent with variants:
        // - Kad, Identify, Ping (from base protocols)
        // - Protocols(NetworkProtocolsEvent) - which has Store and Get variants

        match event {
            SwarmEvent::Behaviour(ref behaviour_event) => {
                // Try to access as a debug string first to understand the structure
                tracing::debug!("Received behaviour event: {:?}", behaviour_event);

                // TODO: Properly match on the generated enum variants
                // For now, let's just log it
            }
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Listening on {}", address)
            }
            _ => {}
        }
    }
}
