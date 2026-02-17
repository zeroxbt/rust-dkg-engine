mod bootstrap;
mod commands;
mod config;
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

use periodic::seed_sharding_tables;

pub async fn run() {
    // Install rustls crypto provider before any TLS connections
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let bootstrap::CoreBootstrap {
        config,
        managers,
        services,
        command_scheduler,
        command_rx,
        mut network_event_loop,
        blockchain_ids,
    } = bootstrap::build_core().await;

    display_rust_dkg_engine_ascii_art();

    // Start listening before spawning the network task
    if let Err(error) = network_event_loop.start_listening() {
        tracing::error!("Failed to start swarm listener: {}", error);
        return;
    }

    if config::is_dev_env() {
        initialize_dev_environment(&managers.blockchain).await;
    }

    // Seed peer registry with sharding tables before commands start
    seed_sharding_tables(
        &managers.blockchain,
        &services.peer_service,
        managers.network.peer_id(),
    )
    .await;

    // Load persisted peer addresses and inject into Kademlia routing table.
    // This must happen after sharding table seeding so dial_peers knows who to connect to.
    bootstrap::hydrate_persisted_peer_addresses(&managers, &services).await;
    let periodic_deps = bootstrap::build_periodic_deps(&managers, &services, &command_scheduler);
    let command_executor =
        bootstrap::build_command_executor(&managers, &services, &command_scheduler, command_rx);
    let controllers =
        bootstrap::build_controllers(&config, &managers, &services, &command_scheduler);

    runtime::run(
        runtime::RuntimeDeps {
            command_scheduler,
            network_manager: Arc::clone(&managers.network),
            peer_service: Arc::clone(&services.peer_service),
            periodic_deps,
            blockchain_ids,
        },
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
