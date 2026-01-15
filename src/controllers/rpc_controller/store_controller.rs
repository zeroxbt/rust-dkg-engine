use std::sync::Arc;

use blockchain::BlockchainManager;
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
    blockchain_manager: Arc<BlockchainManager>,
}

impl StoreController {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            network_manager: Arc::clone(context.network_manager()),
            repository_manager: Arc::clone(context.repository_manager()),
            blockchain_manager: Arc::clone(context.blockchain_manager()),
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

        // TODO: store in pending storage

        */

        let identity_id = self
            .blockchain_manager
            .get_identity_id(&data.blockchain())
            .await
            .unwrap()
            .unwrap();

        let signature = self
            .blockchain_manager
            .sign_message(
                &data.blockchain(),
                data.dataset_root().strip_prefix("0x").unwrap(),
            )
            .await
            .unwrap();
        let signature = blockchain::utils::split_signature(signature).unwrap();

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

        self.network_manager
            .send_protocol_response(ProtocolResponse::Store { channel, message })
            .await
            .unwrap();
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
        {
            self.repository_manager
                .signature_repository()
                .create(
                    header.operation_id,
                    "network",
                    &identity_id.to_string(),
                    signature.v,
                    &signature.r,
                    &signature.s,
                    &signature.vs,
                )
                .await
                .unwrap();
        };

        // Record response using operation manager
        match self
            .publish_service
            .operation_manager()
            .record_response(operation_id.into_inner(), is_success)
            .await
        {
            Ok(crate::services::operation_manager::OperationAction::Complete) => {
                tracing::debug!("Operation {operation_id} completed successfully");
            }
            Ok(crate::services::operation_manager::OperationAction::Failed { reason }) => {
                tracing::debug!("Operation {operation_id} failed: {reason}");
            }
            Ok(crate::services::operation_manager::OperationAction::Continue) => {
                tracing::debug!("Operation {operation_id} continuing (waiting for more responses)");
            }
            Ok(crate::services::operation_manager::OperationAction::AlreadyFinished) => {
                tracing::trace!("Operation {operation_id} already finished (late response)");
            }
            Err(e) => {
                tracing::error!("Failed to record response for operation {operation_id}: {e}");
            }
        }
    }
}
