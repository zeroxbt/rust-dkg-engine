use std::sync::Arc;

use async_trait::async_trait;
use blockchain::BlockchainName;
use futures::future::join_all;
use libp2p::PeerId;
use network::{
    NetworkManager, RequestMessage,
    message::{RequestMessageHeader, RequestMessageType},
};
use repository::RepositoryManager;
use serde::{Deserialize, Serialize};

use crate::{
    commands::command::Command,
    context::Context,
    network::{NetworkProtocols, ProtocolRequest},
    services::{pending_storage_service::PendingStorageService, publish_service::PublishService},
    types::{
        models::OperationId,
        protocol::StoreRequestData,
        traits::{
            command::{CommandData, CommandExecutionResult, CommandHandler},
            service::{NetworkOperationProtocol, OperationLifecycle},
        },
    },
};

#[derive(Serialize, Deserialize, Clone)]
pub struct SendPublishRequestsCommandData {
    operation_id: OperationId,
    blockchain: BlockchainName,
    dataset_root: String,
    minimum_number_of_node_replications: Option<u8>,
}

impl CommandData for SendPublishRequestsCommandData {
    const COMMAND_NAME: &'static str = "sendPublishRequestsCommand";
}

impl SendPublishRequestsCommandData {
    pub fn new(
        operation_id: OperationId,
        blockchain: BlockchainName,
        dataset_root: String,
        minimum_number_of_node_replications: Option<u8>,
    ) -> Self {
        Self {
            operation_id,
            blockchain,
            dataset_root,
            minimum_number_of_node_replications,
        }
    }
}

pub struct SendPublishRequestsCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    pending_storage_service: Arc<PendingStorageService>,
    publish_service: Arc<PublishService>,
}

impl SendPublishRequestsCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            publish_service: Arc::clone(context.publish_service()),
            pending_storage_service: Arc::clone(context.pending_storage_service()),
        }
    }
}

#[async_trait]
impl CommandHandler for SendPublishRequestsCommandHandler {
    fn name(&self) -> &'static str {
        SendPublishRequestsCommandData::COMMAND_NAME
    }

    async fn execute(&self, command: &Command) -> CommandExecutionResult {
        let data = SendPublishRequestsCommandData::from_command(command);

        let SendPublishRequestsCommandData {
            operation_id,
            blockchain,
            dataset_root,
            minimum_number_of_node_replications,
        } = data;

        tracing::debug!(
            "Searching for shard for operation: {operation_id}, dataset root: {dataset_root}"
        );

        let shard_nodes = match self
            .repository_manager
            .shard_repository()
            .get_all_peer_records(blockchain.as_str(), true)
            .await
        {
            Ok(shard_nodes) => shard_nodes,
            Err(e) => {
                let error_message = format!(
                    "Faled to get shard nodes from repository for operation: {operation_id}. Error: {e}"
                );

                if let Err(e) = self
                    .publish_service
                    .mark_failed(operation_id, &error_message)
                    .await
                {
                    tracing::error!("Unable to mark operation {} as failed: {}", operation_id, e)
                }
                return CommandExecutionResult::Completed;
            }
        };

        tracing::debug!(
            "Found {} node(s) for operation: {operation_id}",
            shard_nodes.len()
        );

        let min_ack_responses =
            minimum_number_of_node_replications.unwrap_or(self.publish_service.min_ack_responses());

        if shard_nodes.len() < min_ack_responses as usize {
            let error_message = format!(
                "Unable to find enough nodes for operation: {operation_id}. Minimum number of nodes required: {min_ack_responses}"
            );

            // TODO: in js implementation the operation result data is removed. check how it's
            // handled when it's read by client
            if let Err(e) = self
                .publish_service
                .mark_failed(operation_id, &error_message)
                .await
            {
                tracing::error!("Unable to mark operation {} as failed: {}", operation_id, e)
            }

            return CommandExecutionResult::Completed;
        }

        let dataset = match self.pending_storage_service.get_dataset(operation_id).await {
            Ok(data) => data.dataset().clone(),
            Err(e) => {
                if let Err(e) = self
                    .publish_service
                    .mark_failed(operation_id, &e.to_string())
                    .await
                {
                    tracing::error!("Unable to mark operation {} as failed: {}", operation_id, e)
                }

                return CommandExecutionResult::Completed;
            }
        };

        let message = RequestMessage {
            header: RequestMessageHeader {
                operation_id: operation_id.into_inner(),
                message_type: RequestMessageType::ProtocolRequest,
            },
            data: StoreRequestData::new(
                dataset.public.clone(),
                dataset_root.clone(),
                blockchain.to_string(),
            ),
        };

        let my_peer_id = *self.network_manager.peer_id();
        let mut send_futures = Vec::with_capacity(shard_nodes.len());

        for node in shard_nodes {
            let remote_peer_id: PeerId = node.peer_id.parse().unwrap();
            if remote_peer_id == my_peer_id {
                // TODO:
                //      1. create signature
                //      2. process response for our own node
            } else {
                let network_manager = Arc::clone(&self.network_manager);
                let message = message.clone();

                send_futures.push(async move {
                    if let Err(e) = network_manager
                        .send_protocol_request(ProtocolRequest::Store {
                            peer: remote_peer_id,
                            message,
                        })
                        .await
                    {
                        tracing::error!(
                            "Failed to send protocol request to {}: {}",
                            remote_peer_id,
                            e
                        );
                    }
                });
            }
        }

        join_all(send_futures).await;

        CommandExecutionResult::Completed
    }
}
