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

use commands::{
    executor::CommandExecutor,
    registry::{CommandResolver, CommandResolverDeps},
    scheduler::CommandScheduler,
};
use context::{
    BlockchainEventListenerDeps, ClaimRewardsDeps, CleanupDeps, DialPeersDeps,
    HandleBatchGetRequestDeps, HandleGetRequestDeps, HandlePublishFinalityRequestDeps,
    HandlePublishStoreRequestDeps, ParanetSyncDeps, ProvingDeps, SavePeerAddressesDeps,
    SendGetRequestsDeps, SendPublishFinalityRequestDeps, SendPublishStoreRequestsDeps,
    ShardingTableCheckDeps, SyncDeps,
};
use controllers::{
    http_api_controller::HttpApiDeps,
    rpc_controller::{
        BatchGetRpcControllerDeps, GetRpcControllerDeps, PublishFinalityRpcControllerDeps,
        PublishStoreRpcControllerDeps, RpcRouterDeps,
    },
};
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
    let blockchain_ids = managers
        .blockchain
        .get_blockchain_ids()
        .into_iter()
        .cloned()
        .collect();

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

    let periodic_deps = Arc::new(periodic::PeriodicDeps {
        dial_peers: DialPeersDeps {
            network_manager: Arc::clone(&managers.network),
            peer_service: Arc::clone(&services.peer_service),
        },
        cleanup: CleanupDeps {
            repository_manager: Arc::clone(&managers.repository),
            publish_tmp_dataset_store: Arc::clone(&services.publish_tmp_dataset_store),
            publish_operation_results: Arc::clone(&services.publish_store_operation),
            get_operation_results: Arc::clone(&services.get_operation),
            store_response_channels: Arc::clone(&services.response_channels.store),
            get_response_channels: Arc::clone(&services.response_channels.get),
            finality_response_channels: Arc::clone(&services.response_channels.finality),
            batch_get_response_channels: Arc::clone(&services.response_channels.batch_get),
        },
        save_peer_addresses: SavePeerAddressesDeps {
            peer_service: Arc::clone(&services.peer_service),
            peer_address_store: Arc::clone(&services.peer_address_store),
        },
        sharding_table_check: ShardingTableCheckDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            peer_service: Arc::clone(&services.peer_service),
        },
        blockchain_event_listener: BlockchainEventListenerDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            repository_manager: Arc::clone(&managers.repository),
            command_scheduler: command_scheduler.clone(),
        },
        claim_rewards: ClaimRewardsDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
        },
        proving: ProvingDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            repository_manager: Arc::clone(&managers.repository),
            triple_store_service: Arc::clone(&services.triple_store),
            network_manager: Arc::clone(&managers.network),
            assertion_validation_service: Arc::clone(&services.assertion_validation),
            peer_service: Arc::clone(&services.peer_service),
        },
        sync: SyncDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            repository_manager: Arc::clone(&managers.repository),
            triple_store_service: Arc::clone(&services.triple_store),
            network_manager: Arc::clone(&managers.network),
            assertion_validation_service: Arc::clone(&services.assertion_validation),
            peer_service: Arc::clone(&services.peer_service),
        },
        paranet_sync: ParanetSyncDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            repository_manager: Arc::clone(&managers.repository),
            triple_store_service: Arc::clone(&services.triple_store),
            get_fetch_service: Arc::clone(&services.get_fetch),
        },
    });

    let command_resolver = CommandResolver::new(CommandResolverDeps {
        send_publish_store_requests: SendPublishStoreRequestsDeps {
            network_manager: Arc::clone(&managers.network),
            peer_service: Arc::clone(&services.peer_service),
            blockchain_manager: Arc::clone(&managers.blockchain),
            publish_store_operation_status_service: Arc::clone(&services.publish_store_operation),
            publish_tmp_dataset_store: Arc::clone(&services.publish_tmp_dataset_store),
        },
        handle_publish_store_request: HandlePublishStoreRequestDeps {
            network_manager: Arc::clone(&managers.network),
            blockchain_manager: Arc::clone(&managers.blockchain),
            peer_service: Arc::clone(&services.peer_service),
            store_response_channels: Arc::clone(&services.response_channels.store),
            publish_tmp_dataset_store: Arc::clone(&services.publish_tmp_dataset_store),
        },
        send_publish_finality_request: SendPublishFinalityRequestDeps {
            repository_manager: Arc::clone(&managers.repository),
            network_manager: Arc::clone(&managers.network),
            peer_service: Arc::clone(&services.peer_service),
            blockchain_manager: Arc::clone(&managers.blockchain),
            publish_tmp_dataset_store: Arc::clone(&services.publish_tmp_dataset_store),
            triple_store_service: Arc::clone(&services.triple_store),
        },
        handle_publish_finality_request: HandlePublishFinalityRequestDeps {
            repository_manager: Arc::clone(&managers.repository),
            network_manager: Arc::clone(&managers.network),
            finality_response_channels: Arc::clone(&services.response_channels.finality),
        },
        send_get_requests: SendGetRequestsDeps {
            get_operation_status_service: Arc::clone(&services.get_operation),
            get_fetch_service: Arc::clone(&services.get_fetch),
        },
        handle_get_request: HandleGetRequestDeps {
            network_manager: Arc::clone(&managers.network),
            triple_store_service: Arc::clone(&services.triple_store),
            peer_service: Arc::clone(&services.peer_service),
            get_response_channels: Arc::clone(&services.response_channels.get),
            blockchain_manager: Arc::clone(&managers.blockchain),
        },
        handle_batch_get_request: HandleBatchGetRequestDeps {
            network_manager: Arc::clone(&managers.network),
            triple_store_service: Arc::clone(&services.triple_store),
            peer_service: Arc::clone(&services.peer_service),
            batch_get_response_channels: Arc::clone(&services.response_channels.batch_get),
        },
    });

    let command_executor =
        CommandExecutor::new(command_scheduler.clone(), command_resolver, command_rx);

    let controllers = controllers::initialize(
        &config.http_api,
        &config.rpc,
        RpcRouterDeps {
            publish_store: PublishStoreRpcControllerDeps {
                store_response_channels: Arc::clone(&services.response_channels.store),
                command_scheduler: command_scheduler.clone(),
            },
            get: GetRpcControllerDeps {
                get_response_channels: Arc::clone(&services.response_channels.get),
                command_scheduler: command_scheduler.clone(),
            },
            publish_finality: PublishFinalityRpcControllerDeps {
                finality_response_channels: Arc::clone(&services.response_channels.finality),
                command_scheduler: command_scheduler.clone(),
            },
            batch_get: BatchGetRpcControllerDeps {
                batch_get_response_channels: Arc::clone(&services.response_channels.batch_get),
                command_scheduler: command_scheduler.clone(),
            },
        },
        HttpApiDeps {
            command_scheduler: command_scheduler.clone(),
            repository_manager: Arc::clone(&managers.repository),
            get_operation_status_service: Arc::clone(&services.get_operation),
            publish_store_operation_status_service: Arc::clone(&services.publish_store_operation),
        },
    );

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
