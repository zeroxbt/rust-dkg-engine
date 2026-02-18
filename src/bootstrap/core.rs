use std::sync::Arc;

use dkg_blockchain::BlockchainId;
use dkg_network::{KeyManager, Multiaddr, NetworkEventLoop, PeerId};
use tokio::sync::mpsc;

use crate::{
    bootstrap::{ApplicationDeps, build_application},
    commands::{executor::CommandExecutionRequest, scheduler::CommandScheduler},
    config::{self, AppPaths, Config},
    managers::{self, Managers},
    runtime_state::{self, RuntimeState},
};

pub(crate) struct CoreBootstrap {
    pub(crate) config: Arc<Config>,
    pub(crate) managers: Managers,
    pub(crate) runtime_state: RuntimeState,
    pub(crate) application: ApplicationDeps,
    pub(crate) command_scheduler: CommandScheduler,
    pub(crate) command_rx: mpsc::Receiver<CommandExecutionRequest>,
    pub(crate) network_event_loop: NetworkEventLoop,
    pub(crate) blockchain_ids: Vec<BlockchainId>,
}

pub(crate) async fn build_core() -> CoreBootstrap {
    let config = Arc::new(config::initialize_configuration());
    crate::logger::initialize(&config.logger, &config.telemetry);

    let paths = AppPaths::from_root(config.app_data_path.clone());
    let network_key = KeyManager::load_or_generate(&paths.network_key)
        .await
        .expect("Failed to load or generate network identity key");

    let (command_scheduler, command_rx) = CommandScheduler::channel();
    let (managers, network_event_loop) =
        managers::initialize(&config.managers, &paths, network_key).await;

    let runtime_state = runtime_state::initialize();
    let application = build_application(&managers, &runtime_state);
    let blockchain_ids = managers
        .blockchain
        .get_blockchain_ids()
        .into_iter()
        .cloned()
        .collect();

    CoreBootstrap {
        config,
        managers,
        runtime_state,
        application,
        command_scheduler,
        command_rx,
        network_event_loop,
        blockchain_ids,
    }
}

pub(crate) async fn hydrate_persisted_peer_addresses(managers: &Managers) {
    let peer_address_store = managers.key_value_store.peer_address_store();
    let persisted_addresses = peer_address_store.load_all().await;
    if persisted_addresses.is_empty() {
        return;
    }

    let addresses: Vec<_> = persisted_addresses
        .into_iter()
        .filter_map(|(peer_id_string, addr_strings)| {
            let peer_id: PeerId = peer_id_string.parse().ok()?;
            let addrs: Vec<Multiaddr> = addr_strings
                .into_iter()
                .filter_map(|addr| addr.parse().ok())
                .collect();
            if addrs.is_empty() {
                None
            } else {
                Some((peer_id, addrs))
            }
        })
        .collect();

    if addresses.is_empty() {
        return;
    }

    tracing::info!(
        peers = addresses.len(),
        "Loading persisted peer addresses into Kademlia"
    );

    if let Err(error) = managers.network.add_addresses(addresses).await {
        tracing::warn!(error = %error, "Failed to inject persisted peer addresses");
    }
}
