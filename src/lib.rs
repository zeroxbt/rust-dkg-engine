mod application;
mod bootstrap;
mod commands;
mod config;
mod controllers;
mod error;
mod logger;
mod managers;
mod operations;
mod peer_registry;
mod runtime;
mod tasks;

use std::sync::Arc;

pub use error::NodeError;
use tasks::periodic::seed_sharding_tables;

pub async fn run() -> Result<(), NodeError> {
    // Install rustls crypto provider before any TLS connections
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|error| {
            NodeError::Other(format!(
                "Failed to install rustls crypto provider: {error:?}"
            ))
        })?;

    let bootstrap::CoreBootstrap {
        config,
        managers,
        peer_registry,
        application,
        command_scheduler,
        command_rx,
        mut network_event_loop,
        blockchain_ids,
    } = bootstrap::build_core().await?;

    // Start listening before spawning the network task
    network_event_loop.start_listening()?;

    if config::is_dev_env() {
        initialize_dev_environment(&managers.blockchain).await?;
    }

    // Seed peer registry with sharding tables before commands start
    seed_sharding_tables(
        &managers.blockchain,
        &peer_registry,
        managers.network.peer_id(),
    )
    .await;

    // Load persisted peer addresses and inject into Kademlia routing table.
    // This must happen after sharding table seeding so dial_peers knows who to connect to.
    bootstrap::hydrate_persisted_peer_addresses(&managers, &peer_registry).await;
    let periodic_tasks_deps = bootstrap::build_periodic_tasks_deps(
        &managers,
        &peer_registry,
        &application,
        &command_scheduler,
    );
    let command_executor = bootstrap::build_command_executor(
        &managers,
        &peer_registry,
        &application,
        &command_scheduler,
        command_rx,
    );
    let controllers =
        bootstrap::build_controllers(&config, &managers, &application, &command_scheduler);

    runtime::run(
        runtime::RuntimeDeps {
            command_scheduler,
            network_manager: Arc::clone(&managers.network),
            peer_registry: Arc::clone(&peer_registry),
            periodic_tasks_deps,
            blockchain_ids,
        },
        command_executor,
        network_event_loop,
        controllers.rpc_router,
        controllers.http_router,
        config.runtime.clone(),
        config.periodic_tasks.clone(),
        config.telemetry.metrics.enabled,
    )
    .await;

    Ok(())
}

async fn initialize_dev_environment(
    blockchain_manager: &Arc<dkg_blockchain::BlockchainManager>,
) -> Result<(), NodeError> {
    use dkg_blockchain::parse_ether_to_u128;

    tracing::info!("Initializing dev environment: setting stake and ask...");

    // 50,000 tokens for stake
    let stake_wei: u128 = parse_ether_to_u128("50000")?;
    // 0.2 tokens for ask
    let ask_wei: u128 = parse_ether_to_u128("0.2")?;

    for blockchain_id in blockchain_manager.get_blockchain_ids() {
        blockchain_manager
            .set_stake(blockchain_id, stake_wei)
            .await?;
        blockchain_manager.set_ask(blockchain_id, ask_wei).await?;
    }

    Ok(())
}
