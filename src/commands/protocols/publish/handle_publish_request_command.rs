use std::sync::Arc;

use async_trait::async_trait;
use blockchain::{BlockchainName, utils::SignatureComponents};
use network::{
    NetworkManager, ResponseMessage,
    message::{ResponseMessageHeader, ResponseMessageType},
};
use repository::RepositoryManager;
use serde::{Deserialize, Serialize};

use crate::{
    commands::command::Command,
    context::Context,
    network::{NetworkProtocols, ProtocolResponse, SessionManager},
    services::{pending_storage_service::PendingStorageService, publish_service::PublishService},
    types::{
        models::OperationId,
        protocol::StoreResponseData,
        traits::{
            command::{CommandData, CommandExecutionResult, CommandHandler},
            service::OperationLifecycle,
        },
    },
};

#[derive(Serialize, Deserialize, Clone)]
pub struct HandlePublishRequestCommandData {
    blockchain: BlockchainName,
    operation_id: OperationId,
    dataset_root: String,
    remote_peer_id: String,
}

impl CommandData for HandlePublishRequestCommandData {
    const COMMAND_NAME: &'static str = "handlePublishRequestCommand";
}

impl HandlePublishRequestCommandData {
    pub fn new(
        blockchain: BlockchainName,
        operation_id: OperationId,
        dataset_root: String,
        remote_peer_id: String,
    ) -> Self {
        Self {
            blockchain,
            operation_id,
            dataset_root,
            remote_peer_id,
        }
    }
}

pub struct HandlePublishRequestCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    publish_service: Arc<PublishService>,
    pending_storage_service: Arc<PendingStorageService>,
    session_manager: Arc<SessionManager<StoreResponseData>>,
}

impl HandlePublishRequestCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            publish_service: Arc::clone(context.publish_service()),
            pending_storage_service: Arc::clone(context.pending_storage_service()),
            session_manager: Arc::clone(context.store_session_manager()),
        }
    }
}

#[async_trait]
impl CommandHandler for HandlePublishRequestCommandHandler {
    fn name(&self) -> &'static str {
        HandlePublishRequestCommandData::COMMAND_NAME
    }

    async fn execute(&self, command: &Command) -> CommandExecutionResult {
        let data = HandlePublishRequestCommandData::from_command(command);

        let HandlePublishRequestCommandData {
            blockchain,
            operation_id,
            dataset_root,
            remote_peer_id,
        } = data;

        // Retrieve the cached channel
        let channel = match self
            .session_manager
            .retrieve_channel(&remote_peer_id, operation_id)
        {
            Some(channel) => channel,
            None => {
                tracing::error!(
                    "No cached session found for peer: {}, operation_id: {}. Session may have expired.",
                    remote_peer_id,
                    operation_id
                );
                return CommandExecutionResult::Completed;
            }
        };

        let _dataset = match self.pending_storage_service.get_dataset(operation_id).await {
            Ok(data) => data.dataset().clone(),
            Err(e) => {
                if let Err(e) = self
                    .publish_service
                    .mark_failed(operation_id, &e.to_string())
                    .await
                {
                    tracing::error!("Unable to mark operation {} as failed: {}", operation_id, e)
                }

                // Send NACK response
                let message = ResponseMessage {
                    header: ResponseMessageHeader {
                        operation_id: operation_id.into_inner(),
                        message_type: ResponseMessageType::Nack,
                    },
                    data: StoreResponseData::Error {
                        error_message: format!("Failed to get dataset: {}", e),
                    },
                };

                let _ = self
                    .network_manager
                    .send_protocol_response(ProtocolResponse::Store { channel, message })
                    .await;

                return CommandExecutionResult::Completed;
            }
        };

        tracing::trace!(
            "Validating shard for datasetRoot: {dataset_root}, operation: {operation_id}"
        );

        match self
            .repository_manager
            .shard_repository()
            .get_peer_record(blockchain.as_str(), &remote_peer_id)
            .await
        {
            Ok(Some(_)) => {}
            invalid_result => {
                let error_message = match invalid_result {
                    Err(e) => format!(
                        "Failed to get remote peer: {remote_peer_id} in shard_repository for operation: {operation_id}. Error: {e}"
                    ),
                    _ => format!(
                        "Invalid shard on blockchain: {blockchain}, operation: {operation_id}"
                    ),
                };

                if let Err(e) = self
                    .publish_service
                    .mark_failed(operation_id, &error_message)
                    .await
                {
                    tracing::error!("Unable to mark operation {} as failed: {}", operation_id, e)
                }

                let message = ResponseMessage {
                    header: ResponseMessageHeader {
                        operation_id: operation_id.into_inner(),
                        message_type: ResponseMessageType::Nack,
                    },
                    data: StoreResponseData::Error {
                        error_message: "Invalid neighbourhood".to_string(),
                    },
                };

                let _ = self
                    .network_manager
                    .send_protocol_response(ProtocolResponse::Store { channel, message })
                    .await;

                return CommandExecutionResult::Completed;
            }
        };

        /*

        // TODO: validate dataset root

        // TODO: get identity id

        // TODO: sign message

        // TODO send ACK response with signature data

        return {
            messageType: NETWORK_MESSAGE_TYPES.RESPONSES.ACK,
            messageData: { identityId, v, r, s, vs },
        }; */

        // For now, send a simple ACK (you'll replace this with actual signature logic)
        let message = ResponseMessage {
            header: ResponseMessageHeader {
                operation_id: operation_id.into_inner(),
                message_type: ResponseMessageType::Ack,
            },
            data: StoreResponseData::Data {
                // TODO: Add actual signature data here
                identity_id: String::new(),
                signature: SignatureComponents {
                    v: 0,
                    r: String::new(),
                    s: String::new(),
                    vs: String::new(),
                },
            },
        };

        let _ = self
            .network_manager
            .send_protocol_response(ProtocolResponse::Store { channel, message })
            .await;

        CommandExecutionResult::Completed
    }
}
