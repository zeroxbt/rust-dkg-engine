use std::sync::Arc;

use blockchain::utils::SignatureComponents;
use network::{
    NetworkManager, PeerId,
    message::{RequestMessage, ResponseMessage, ResponseMessageHeader, ResponseMessageType},
    request_response,
};
use repository::RepositoryManager;

use crate::{
    context::Context,
    network::{NetworkProtocols, ProtocolResponse},
    services::{pending_storage_service::PendingStorageService, publish_service::PublishService},
    types::{
        models::OperationId,
        protocol::{StoreRequestData, StoreResponseData},
    },
};

pub struct StoreController {
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    publish_service: Arc<PublishService>,
    pending_storage_service: Arc<PendingStorageService>,
    repository_manager: Arc<RepositoryManager>,
}

impl StoreController {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            network_manager: Arc::clone(context.network_manager()),
            repository_manager: Arc::clone(context.repository_manager()),
            publish_service: Arc::clone(context.publish_service()),
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

                return;
            }
        };

        tracing::trace!(
            "Validating shard for datasetRoot: {}, operation: {operation_id}",
            data.dataset_root()
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

                return;
            }
        };

        /*

        // TODO: validate dataset root

        // TODO: get identity id

        // TODO: sign message

        */

        // For now, send a simple ACK ( TODO: replace this with actual signature logic)
        let message = ResponseMessage {
            header: ResponseMessageHeader {
                operation_id: operation_id.into_inner(),
                message_type: ResponseMessageType::Ack,
            },
            data: StoreResponseData::Data {
                // TODO: Add actual signature data here
                identity_id: 0,
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
    }

    pub async fn handle_response(
        &self,
        response: ResponseMessage<StoreResponseData>,
        _peer: PeerId,
    ) {
        let ResponseMessage { header, data } = response;

        match (&header.message_type, data.clone()) {
            (ResponseMessageType::Ack, _)
            | (ResponseMessageType::Nack | ResponseMessageType::Busy, _) => {
                let operation_id = OperationId::from(header.operation_id);

                self.publish_service
                    .response_tracker()
                    .update_state_responses(
                        &header.operation_id,
                        header.message_type == ResponseMessageType::Ack,
                    )
                    .await;

                let operation_state = self
                    .publish_service
                    .response_tracker()
                    .get_state(&header.operation_id)
                    .await;

                let total_responses =
                    operation_state.completed_number + operation_state.failed_number;

                tracing::debug!(
                    "Processing response for operation id: {}, number of responses: {}, Completed: {}, Failed: {}, minimum replication factor: {}",
                    header.operation_id,
                    total_responses,
                    operation_state.completed_number,
                    operation_state.failed_number,
                    operation_state.min_ack_responses
                );

                if operation_state.completed_number > operation_state.min_ack_responses {
                    return;
                }

                if operation_state.completed_number == operation_state.min_ack_responses {
                    tracing::info!(
                        "[PUBLISH] Minimum replication reached for operation: {},  completed: {}/{} ",
                        header.operation_id,
                        operation_state.completed_number,
                        operation_state.min_ack_responses
                    );

                    self.publish_service
                        .mark_completed(operation_id)
                        .await
                        .unwrap();

                    tracing::info!(
                        "Total number of responses: {}, failed: {}, completed: {}",
                        total_responses,
                        operation_state.failed_number,
                        operation_state.completed_number
                    );
                } else if total_responses == operation_state.nodes_found.len() as u8 {
                    tracing::warn!(
                        "[PUBLISH] Failed for operation: {}, only {}/{} nodes responsed successfully",
                        header.operation_id,
                        operation_state.completed_number,
                        operation_state.min_ack_responses
                    );
                    self.publish_service
                        .mark_failed(operation_id, "Not replicated to enough nodes!")
                        .await
                        .unwrap();
                    tracing::info!(
                        "Total number of responses: {}, failed: {}, completed: {}",
                        total_responses,
                        operation_state.failed_number,
                        operation_state.completed_number
                    );
                }
            }
        }
    }
}
