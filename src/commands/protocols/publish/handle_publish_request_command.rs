use std::sync::Arc;

use async_trait::async_trait;
use blockchain::{BlockchainId, BlockchainManager};
use libp2p::PeerId;
use network::{
    NetworkManager, ResponseMessage,
    message::{ResponseMessageHeader, ResponseMessageType},
    request_response::ResponseChannel,
};
use repository::RepositoryManager;
use uuid::Uuid;
use validation::ValidationManager;

use crate::{
    commands::command::Command,
    context::Context,
    network::{NetworkProtocols, ProtocolResponse, SessionManager},
    services::operation_manager::OperationManager,
    types::{
        models::Assertion,
        protocol::StoreResponseData,
        traits::command::{CommandExecutionResult, CommandHandler},
    },
};

/// Command data for handling incoming publish/store requests.
/// Dataset is passed inline; channel is retrieved from session manager.
pub struct HandlePublishRequestCommandData {
    pub blockchain: BlockchainId,
    pub operation_id: Uuid,
    pub dataset_root: String,
    pub remote_peer_id: PeerId,
    pub dataset: Assertion,
}

impl HandlePublishRequestCommandData {
    pub fn new(
        blockchain: BlockchainId,
        operation_id: Uuid,
        dataset_root: String,
        remote_peer_id: PeerId,
        dataset: Assertion,
    ) -> Self {
        Self {
            blockchain,
            operation_id,
            dataset_root,
            remote_peer_id,
            dataset,
        }
    }
}

pub struct HandlePublishRequestCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    publish_operation_manager: Arc<OperationManager>,
    validation_manager: Arc<ValidationManager>,
    blockchain_manager: Arc<BlockchainManager>,
    session_manager: Arc<SessionManager<StoreResponseData>>,
}

impl HandlePublishRequestCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            validation_manager: Arc::clone(context.validation_manager()),
            publish_operation_manager: Arc::clone(context.publish_operation_manager()),
            session_manager: Arc::clone(context.store_session_manager()),
        }
    }

    async fn send_response(
        &self,
        channel: ResponseChannel<ResponseMessage<StoreResponseData>>,
        operation_id: Uuid,
        message: ResponseMessage<StoreResponseData>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_protocol_response(ProtocolResponse::Store { channel, message })
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send response"
            );
        }
    }

    async fn send_nack(
        &self,
        channel: ResponseChannel<ResponseMessage<StoreResponseData>>,
        operation_id: Uuid,
        error_message: &str,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader {
                operation_id,
                message_type: ResponseMessageType::Nack,
            },
            data: StoreResponseData::Error {
                error_message: error_message.to_string(),
            },
        };
        self.send_response(channel, operation_id, message).await;
    }
}

#[async_trait]
impl CommandHandler for HandlePublishRequestCommandHandler {
    fn name(&self) -> &'static str {
        "handlePublishRequestCommand"
    }

    async fn execute(&self, command: &Command) -> CommandExecutionResult {
        let data = command.data::<HandlePublishRequestCommandData>();

        let operation_id = data.operation_id;
        let blockchain = &data.blockchain;
        let dataset_root = &data.dataset_root;
        let remote_peer_id = &data.remote_peer_id;
        let dataset = &data.dataset;

        // Retrieve the channel from session manager
        let Some(channel) = self
            .session_manager
            .retrieve_channel(remote_peer_id, operation_id)
        else {
            tracing::error!(
                operation_id = %operation_id,
                peer = %remote_peer_id,
                "No cached session found. Session may have expired."
            );
            return CommandExecutionResult::Completed;
        };

        match self
            .repository_manager
            .shard_repository()
            .get_peer_record(blockchain.as_str(), &remote_peer_id.to_base58())
            .await
        {
            Ok(Some(_)) => {}
            invalid_result => {
                let error_message = match invalid_result {
                    Err(e) => format!(
                        "Failed to get remote peer: {remote_peer_id} in shard_repository for operation: {operation_id}. Error: {e}"
                    ),
                    _ => format!(
                        "Invalid shard on blockchain: {}, operation: {operation_id}",
                        blockchain.as_str()
                    ),
                };

                self.publish_operation_manager
                    .mark_failed(operation_id, error_message)
                    .await;

                self.send_nack(channel, operation_id, "Invalid neighbourhood")
                    .await;

                return CommandExecutionResult::Completed;
            }
        };

        let computed_dataset_root = self
            .validation_manager
            .calculate_merkle_root(&dataset.public);

        if *dataset_root != computed_dataset_root {
            self.send_nack(
                channel,
                operation_id,
                &format!(
                    "Dataset root validation failed. Received dataset root: {}; Calculated dataset root: {}",
                    dataset_root, computed_dataset_root
                ),
            )
            .await;

            return CommandExecutionResult::Completed;
        }

        let identity_id = match self.blockchain_manager.get_identity_id(blockchain).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                self.send_nack(
                    channel,
                    operation_id,
                    &format!("Identity ID not found for blockchain {}", blockchain),
                )
                .await;

                return CommandExecutionResult::Completed;
            }
            Err(e) => {
                self.send_nack(
                    channel,
                    operation_id,
                    &format!("Failed to get identity ID: {}", e),
                )
                .await;

                return CommandExecutionResult::Completed;
            }
        };

        let dataset_root_hex = match dataset_root.strip_prefix("0x") {
            Some(hex) => hex,
            None => {
                self.send_nack(channel, operation_id, "Dataset root missing '0x' prefix")
                    .await;

                return CommandExecutionResult::Completed;
            }
        };

        let signature = match self
            .blockchain_manager
            .sign_message(blockchain, dataset_root_hex)
            .await
        {
            Ok(sig) => sig,
            Err(e) => {
                self.send_nack(
                    channel,
                    operation_id,
                    &format!("Failed to sign message: {}", e),
                )
                .await;

                return CommandExecutionResult::Completed;
            }
        };

        let signature = match blockchain::utils::split_signature(signature) {
            Ok(sig) => sig,
            Err(e) => {
                self.send_nack(
                    channel,
                    operation_id,
                    &format!("Failed to process signature: {}", e),
                )
                .await;

                return CommandExecutionResult::Completed;
            }
        };

        let message = ResponseMessage {
            header: ResponseMessageHeader {
                operation_id,
                message_type: ResponseMessageType::Ack,
            },
            data: StoreResponseData::Data {
                identity_id,
                signature,
            },
        };

        self.send_response(channel, operation_id, message).await;

        tracing::debug!(
            operation_id = %operation_id,
            peer = %remote_peer_id,
            "Store request validated, ACK sent"
        );

        CommandExecutionResult::Completed
    }
}
