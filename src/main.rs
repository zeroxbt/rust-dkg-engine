#![allow(dead_code, unused_variables)]

mod commands;
mod config;
mod constants;
mod context;
mod controllers;

use commands::command::CommandTrait;
use commands::command_executor::CommandExecutor;
use context::Context;
use controllers::http_api::http_api_router::HttpApiRouter;
use controllers::rpc::rpc_router::RpcRouter;
use network::command::NetworkCommand;
use network::NetworkEvent;
use network::NetworkManager;
use repository::RepositoryManager;
use std::sync::Arc;
use tokio::join;

#[tokio::main]
async fn main() {
    let filter = tracing_subscriber::EnvFilter::new(
        "trace,axum=off,sea_orm=off,sqlx=off,rustls=off,hyper=off,libp2p_tcp=off,libp2p_noise=off,yamux=off",
    );
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config = config::initialize_configuration();

    let (network_command_tx, network_command_rx) =
        tokio::sync::mpsc::channel::<NetworkCommand>(1000);
    let (network_event_tx, network_event_rx) = tokio::sync::mpsc::channel::<NetworkEvent>(1000);
    let (schedule_command_tx, schedule_command_rx) =
        tokio::sync::mpsc::channel::<Box<dyn CommandTrait>>(1000);

    let network_manager = NetworkManager::new(config.managers.network).await;
    let repository_manager = RepositoryManager::new(config.managers.repository)
        .await
        .unwrap();

    let context = Arc::new(Context::new(
        network_command_tx.clone(),
        schedule_command_tx,
        repository_manager,
    ));

    let command_executor = Arc::new(CommandExecutor::new(Arc::clone(&context)).await);

    let http_api_router = HttpApiRouter::new(config.http_api, Arc::clone(&context));
    let rpc_router = RpcRouter::new();

    let cloned_command_executor = Arc::clone(&command_executor);

    let schedule_commands_task = tokio::task::spawn(async move {
        cloned_command_executor
            .schedule_commands(schedule_command_rx)
            .await
    });
    let execute_commands_task =
        tokio::task::spawn(async move { command_executor.execute_commands().await });

    let handle_http_events_task =
        tokio::task::spawn(async move { http_api_router.handle_http_requests().await });
    let handle_network_events_task = tokio::task::spawn(async move {
        rpc_router
            .handle_network_events(network_event_rx, network_command_tx)
            .await
    });
    let handle_swarm_task = tokio::task::spawn(async move {
        network_manager
            .handle_swarm(network_command_rx, network_event_tx)
            .await;
    });

    let _ = join!(
        handle_http_events_task,
        handle_network_events_task,
        schedule_commands_task,
        execute_commands_task,
        handle_swarm_task
    );
}
