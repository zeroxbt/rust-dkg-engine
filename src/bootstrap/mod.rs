use std::sync::Arc;

use dkg_blockchain::BlockchainId;
use dkg_network::{KeyManager, Multiaddr, NetworkEventLoop, PeerId};
use tokio::sync::mpsc;

use crate::{
    commands::{
        HandleBatchGetRequestDeps, HandleGetRequestDeps, HandlePublishFinalityRequestDeps,
        HandlePublishStoreRequestDeps, SendGetRequestsDeps, SendPublishFinalityRequestDeps,
        SendPublishStoreRequestsDeps,
        executor::{CommandExecutionRequest, CommandExecutor},
        registry::{CommandResolver, CommandResolverDeps},
        scheduler::CommandScheduler,
    },
    config::{self, AppPaths, Config},
    controllers::{
        self,
        http_api_controller::HttpApiDeps,
        rpc_controller::{
            BatchGetRpcControllerDeps, GetRpcControllerDeps, PublishFinalityRpcControllerDeps,
            PublishStoreRpcControllerDeps, RpcRouterDeps,
        },
    },
    managers::{self, Managers},
    periodic_tasks::{
        self, BlockchainEventListenerDeps, ClaimRewardsDeps, CleanupDeps, DialPeersDeps,
        ParanetSyncDeps, ProvingDeps, SavePeerAddressesDeps, ShardingTableCheckDeps, SyncDeps,
    },
    services::{self, Services},
};

pub(crate) struct CoreBootstrap {
    pub(crate) config: Arc<Config>,
    pub(crate) managers: Managers,
    pub(crate) services: Services,
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

    let services = services::initialize(&managers);
    let blockchain_ids = managers
        .blockchain
        .get_blockchain_ids()
        .into_iter()
        .cloned()
        .collect();

    CoreBootstrap {
        config,
        managers,
        services,
        command_scheduler,
        command_rx,
        network_event_loop,
        blockchain_ids,
    }
}

pub(crate) async fn hydrate_persisted_peer_addresses(managers: &Managers, services: &Services) {
    let persisted_addresses = services.peer_address_store.load_all().await;
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

pub(crate) fn build_periodic_tasks_deps(
    managers: &Managers,
    services: &Services,
    command_scheduler: &CommandScheduler,
) -> Arc<periodic_tasks::PeriodicTasksDeps> {
    Arc::new(periodic_tasks::PeriodicTasksDeps {
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
    })
}

pub(crate) fn build_command_executor(
    managers: &Managers,
    services: &Services,
    command_scheduler: &CommandScheduler,
    command_rx: mpsc::Receiver<CommandExecutionRequest>,
) -> CommandExecutor {
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

    CommandExecutor::new(command_scheduler.clone(), command_resolver, command_rx)
}

pub(crate) fn build_controllers(
    config: &Config,
    managers: &Managers,
    services: &Services,
    command_scheduler: &CommandScheduler,
) -> controllers::Controllers {
    controllers::initialize(
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
    )
}
