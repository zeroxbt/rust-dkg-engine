use std::sync::Arc;

use blockchain::BlockchainManager;
use network::{
    NetworkManager, PeerId,
    message::{RequestMessage, ResponseMessage, ResponseMessageHeader, ResponseMessageType},
    request_response,
};
use repository::RepositoryManager;
use validation::ValidationManager;

use crate::{
    context::Context,
    network::{NetworkProtocols, ProtocolResponse},
    services::{
        operation_manager::OperationManager, pending_storage_service::PendingStorageService,
    },
    types::{
        models::{Assertion, OperationId},
        protocol::{StoreRequestData, StoreResponseData},
    },
};

pub struct StoreController {
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    publish_operation_manager: Arc<OperationManager>,
    pending_storage_service: Arc<PendingStorageService>,
    repository_manager: Arc<RepositoryManager>,
    blockchain_manager: Arc<BlockchainManager>,
    validation_manager: Arc<ValidationManager>,
}

impl StoreController {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            network_manager: Arc::clone(context.network_manager()),
            repository_manager: Arc::clone(context.repository_manager()),
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            validation_manager: Arc::clone(context.validation_manager()),
            publish_operation_manager: Arc::clone(context.publish_operation_manager()),
            pending_storage_service: Arc::clone(context.pending_storage_service()),
        }
    }

    pub async fn handle_request(
        &self,
        request: RequestMessage<StoreRequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<StoreResponseData>>,
        remote_peer_id: PeerId,
    ) {
        let RequestMessage { header, data } = request;

        let operation_id = OperationId::from(header.operation_id);

        tracing::trace!(
            operation_id = %operation_id,
            dataset_root = %data.dataset_root(),
            peer = %remote_peer_id,
            "Store request received"
        );

        match self
            .repository_manager
            .shard_repository()
            .get_peer_record(data.blockchain().as_str(), &remote_peer_id.to_base58())
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
                        data.blockchain().as_str()
                    ),
                };

                self.publish_operation_manager
                    .mark_failed(operation_id.into_inner(), error_message)
                    .await;

                let message = ResponseMessage {
                    header: ResponseMessageHeader {
                        operation_id: operation_id.into_inner(),
                        message_type: ResponseMessageType::Nack,
                    },
                    data: StoreResponseData::Error {
                        error_message: "Invalid neighbourhood".to_string(),
                    },
                };

                if let Err(e) = self
                    .network_manager
                    .send_protocol_response(ProtocolResponse::Store { channel, message })
                    .await
                {
                    tracing::error!(
                        operation_id = %operation_id,
                        error = %e,
                        "Failed to send NACK response"
                    );
                }

                return;
            }
        };

        let dataset_root = self
            .validation_manager
            .calculate_merkle_root(data.dataset());

        if data.dataset_root() != dataset_root {
            let message = ResponseMessage {
                header: ResponseMessageHeader {
                    operation_id: operation_id.into_inner(),
                    message_type: ResponseMessageType::Nack,
                },
                data: StoreResponseData::Error {
                    error_message: format!(
                        "Dataset root validation failed. Received dataset root: {}; Calculated dataset root: {}",
                        data.dataset_root(),
                        dataset_root
                    ),
                },
            };

            if let Err(e) = self
                .network_manager
                .send_protocol_response(ProtocolResponse::Store { channel, message })
                .await
            {
                tracing::error!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to send NACK response"
                );
            }

            return;
        }

        if let Err(e) = self
            .pending_storage_service
            .store_dataset(
                operation_id,
                &dataset_root,
                &Assertion {
                    public: data.dataset().to_owned(),
                    private: None,
                },
            )
            .await
        {
            self.send_error_response(channel, operation_id, &format!("Failed to store dataset: {}", e))
                .await;
            return;
        }

        let identity_id = match self
            .blockchain_manager
            .get_identity_id(&data.blockchain())
            .await
        {
            Ok(Some(id)) => id,
            Ok(None) => {
                self.send_error_response(
                    channel,
                    operation_id,
                    &format!("Identity ID not found for blockchain {}", data.blockchain()),
                )
                .await;
                return;
            }
            Err(e) => {
                self.send_error_response(
                    channel,
                    operation_id,
                    &format!("Failed to get identity ID: {}", e),
                )
                .await;
                return;
            }
        };

        let dataset_root_hex = match data.dataset_root().strip_prefix("0x") {
            Some(hex) => hex,
            None => {
                self.send_error_response(channel, operation_id, "Dataset root missing '0x' prefix")
                    .await;
                return;
            }
        };

        let signature = match self
            .blockchain_manager
            .sign_message(&data.blockchain(), dataset_root_hex)
            .await
        {
            Ok(sig) => sig,
            Err(e) => {
                self.send_error_response(
                    channel,
                    operation_id,
                    &format!("Failed to sign message: {}", e),
                )
                .await;
                return;
            }
        };

        let signature = match blockchain::utils::split_signature(signature) {
            Ok(sig) => sig,
            Err(e) => {
                self.send_error_response(
                    channel,
                    operation_id,
                    &format!("Failed to process signature: {}", e),
                )
                .await;
                return;
            }
        };

        let message = ResponseMessage {
            header: ResponseMessageHeader {
                operation_id: operation_id.into_inner(),
                message_type: ResponseMessageType::Ack,
            },
            data: StoreResponseData::Data {
                identity_id,
                signature,
            },
        };

        if let Err(e) = self
            .network_manager
            .send_protocol_response(ProtocolResponse::Store { channel, message })
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send ACK response"
            );
        } else {
            tracing::debug!(
                operation_id = %operation_id,
                peer = %remote_peer_id,
                "Store request validated, ACK sent"
            );
        }
    }

    async fn send_error_response(
        &self,
        channel: request_response::ResponseChannel<ResponseMessage<StoreResponseData>>,
        operation_id: OperationId,
        error_message: &str,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader {
                operation_id: operation_id.into_inner(),
                message_type: ResponseMessageType::Nack,
            },
            data: StoreResponseData::Error {
                error_message: error_message.to_string(),
            },
        };

        if let Err(e) = self
            .network_manager
            .send_protocol_response(ProtocolResponse::Store { channel, message })
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send error response"
            );
        }
    }

    pub async fn handle_response(
        &self,
        response: ResponseMessage<StoreResponseData>,
        _peer: PeerId,
    ) {
        let ResponseMessage { header, data } = response;

        let operation_id = OperationId::from(header.operation_id);
        let is_success = header.message_type == ResponseMessageType::Ack;

        if let (
            ResponseMessageType::Ack,
            StoreResponseData::Data {
                identity_id,
                signature,
            },
        ) = (header.message_type, data)
            && let Err(e) = self
                .repository_manager
                .signature_repository()
                .store_network_signature(
                    header.operation_id,
                    &identity_id.to_string(),
                    signature.v,
                    &signature.r,
                    &signature.s,
                    &signature.vs,
                )
                .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to store network signature"
            );
        };

        // Record response using operation manager
        if let Err(e) = self
            .publish_operation_manager
            .record_response(operation_id.into_inner(), is_success)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to record response"
            );
        } else {
            tracing::debug!(
                operation_id = %operation_id,
                success = is_success,
                "Store response processed"
            );
        }
    }
}
