use crate::handlers::rpc_handler::{
    base_handler::BaseHandler, get_handler::GetHandler, store_handler::StoreHandler,
};
use network::{
    command::NetworkCommand, identify, request_response, BehaviourEvent, NetworkEvent, SwarmEvent,
};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, error, info};

pub struct RpcRouter {
    get_handler: Arc<GetHandler>,
    store_handler: Arc<StoreHandler>,
}

impl RpcRouter {
    pub fn new() -> Self {
        RpcRouter {
            get_handler: Arc::new(GetHandler {}),
            store_handler: Arc::new(StoreHandler {}),
        }
    }

    pub async fn handle_network_events(
        &self,
        mut network_event_rx: Receiver<NetworkEvent>,
        network_command_tx: Sender<NetworkCommand>,
    ) {
        loop {
            if let Some(event) = network_event_rx.recv().await {
                match event {
                    SwarmEvent::Behaviour(BehaviourEvent::Store(inner_event)) => {
                        match inner_event {
                            request_response::Event::OutboundFailure { error, .. } => {
                                error!("Failed to store: {}", error);
                            }
                            request_response::Event::Message { message, peer } => {
                                self.store_handler
                                    .handle_message(&network_command_tx, message, peer)
                                    .await;
                            }
                            _ => {}
                        }
                    }
                    SwarmEvent::Behaviour(BehaviourEvent::Get(inner_event)) => match inner_event {
                        request_response::Event::OutboundFailure { error, .. } => {
                            error!("Failed to get: {}", error)
                        }
                        request_response::Event::Message { peer, message } => {
                            self.get_handler
                                .handle_message(&network_command_tx, message, peer)
                                .await;
                        }
                        _ => {}
                    },
                    SwarmEvent::NewListenAddr { address, .. } => {
                        info!("Listening on {}", address)
                    }
                    SwarmEvent::Behaviour(BehaviourEvent::Identify(
                        identify::Event::Received { peer_id, info },
                    )) => {
                        network_command_tx
                            .send(NetworkCommand::AddAddress {
                                peer_id,
                                addresses: info.listen_addrs,
                            })
                            .await
                            .unwrap();
                    }
                    e => debug!("found event: {:?}", e),
                }
            }
        }
    }
}
