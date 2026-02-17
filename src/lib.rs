mod commands;
mod config;
mod context;
mod controllers;
mod error;
mod logger;
mod managers;
mod operations;
mod periodic;
mod runtime;
mod services;
mod state;

use std::sync::Arc;

use commands::{executor::CommandExecutor, scheduler::CommandScheduler};
use context::Context;
use dkg_network::{KeyManager, Multiaddr, PeerId};
use periodic::seed_sharding_tables;

use crate::config::AppPaths;

pub async fn run() {
    // Install rustls crypto provider before any TLS connections
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Load configuration first, then initialize logger with config settings
    let config = Arc::new(config::initialize_configuration());
    logger::initialize(&config.logger, &config.telemetry);

    display_rust_dkg_engine_ascii_art();

    // Derive all filesystem paths from the root data directory
    let paths = AppPaths::from_root(config.app_data_path.clone());

    // Load or generate network identity key (security-critical, handled at app level)
    let network_key = KeyManager::load_or_generate(&paths.network_key)
        .await
        .expect("Failed to load or generate network identity key");

    // Create command scheduler channel
    let (command_scheduler, command_rx) = CommandScheduler::channel();
    // Initialize managers and services
    let (managers, mut network_event_loop) =
        managers::initialize(&config.managers, &paths, network_key).await;

    // Start listening before spawning the network task
    if let Err(error) = network_event_loop.start_listening() {
        tracing::error!("Failed to start swarm listener: {}", error);
        return;
    }

    if config::is_dev_env() {
        initialize_dev_environment(&managers.blockchain).await;
    }

    let services = services::initialize(&managers);

    // Seed peer registry with sharding tables before commands start
    seed_sharding_tables(
        &managers.blockchain,
        &services.peer_service,
        managers.network.peer_id(),
    )
    .await;

    // Load persisted peer addresses and inject into Kademlia routing table.
    // This must happen after sharding table seeding so dial_peers knows who to connect to.
    let persisted_addresses = services.peer_address_store.load_all().await;
    if !persisted_addresses.is_empty() {
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

        if !addresses.is_empty() {
            tracing::info!(
                peers = addresses.len(),
                "Loading persisted peer addresses into Kademlia"
            );
            if let Err(e) = managers.network.add_addresses(addresses).await {
                tracing::warn!(error = %e, "Failed to inject persisted peer addresses");
            }
        }
    }

    let context = Arc::new(Context::new(command_scheduler, managers, services));

    let command_executor = CommandExecutor::new(Arc::clone(&context), command_rx);

    let controllers = controllers::initialize(&config.http_api, &config.rpc, &context);

    runtime::run(
        context,
        command_executor,
        network_event_loop,
        controllers.rpc_router,
        controllers.http_router,
        config.cleanup.clone(),
        config.sync.clone(),
        config.paranet_sync.clone(),
        config.proving.clone(),
    )
    .await;
}

fn display_rust_dkg_engine_ascii_art() {
    tracing::info!("██████╗ ██╗  ██╗ ██████╗     ██╗   ██╗ █████╗ ");
    tracing::info!("██╔══██╗██║ ██╔╝██╔════╝     ██║   ██║██╔══██╗");
    tracing::info!("██║  ██║█████╔╝ ██║  ███╗    ██║   ██║╚█████╔╝");
    tracing::info!("██║  ██║██╔═██╗ ██║   ██║    ╚██╗ ██╔╝██╔══██╗");
    tracing::info!("██████╔╝██║  ██╗╚██████╔╝     ╚████╔╝ ╚█████╔╝");
    tracing::info!("╚═════╝ ╚═╝  ╚═╝ ╚═════╝       ╚═══╝   ╚════╝ ");

    tracing::info!("======================================================");
    tracing::info!(
        "             Rust DKG Engine v{}",
        env!("CARGO_PKG_VERSION")
    );
    tracing::info!("======================================================");
    let environment = config::current_env();
    tracing::info!("Node is running in {} environment", environment);
}

async fn initialize_dev_environment(blockchain_manager: &Arc<dkg_blockchain::BlockchainManager>) {
    use dkg_blockchain::parse_ether_to_u128;

    tracing::info!("Initializing dev environment: setting stake and ask...");

    // 50,000 tokens for stake
    let stake_wei: u128 = parse_ether_to_u128("50000").expect("Failed to parse stake amount");
    // 0.2 tokens for ask
    let ask_wei: u128 = parse_ether_to_u128("0.2").expect("Failed to parse ask amount");

    for blockchain_id in blockchain_manager.get_blockchain_ids() {
        if let Err(e) = blockchain_manager.set_stake(blockchain_id, stake_wei).await {
            tracing::error!("Failed to set stake for {}: {}", blockchain_id, e);
            panic!("set-stake did not complete successfully: {}", e);
        }

        if let Err(e) = blockchain_manager.set_ask(blockchain_id, ask_wei).await {
            tracing::error!("Failed to set ask for {}: {}", blockchain_id, e);
            panic!("set-ask did not complete successfully: {}", e);
        }
    }
}
