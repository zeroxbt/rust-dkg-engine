use std::sync::Arc;

use axum::{Json, extract::State, response::IntoResponse};
use blockchain::{H256, Token};
use futures::future::join_all;
use hyper::StatusCode;
use libp2p::PeerId;
use network::{
    RequestMessage,
    message::{RequestMessageHeader, RequestMessageType},
};
use validator::Validate;

use crate::{
    context::Context,
    network::ProtocolRequest,
    types::{
        dto::publish::{PublishRequest, PublishResponse},
        models::OperationId,
        protocol::StoreRequestData,
    },
};

pub struct PublishController;

impl PublishController {
    pub async fn handle_request(
        State(context): State<Arc<Context>>,
        Json(req): Json<PublishRequest>,
    ) -> impl IntoResponse {
        match req.validate() {
            Ok(_) => {
                // Generate operation ID and create DB record immediately
                let operation_id = OperationId::new();

                // Create operation record in DB before spawning
                if let Err(e) = context
                    .publish_service()
                    .operation_manager()
                    .create_operation(operation_id.into_inner())
                    .await
                {
                    tracing::error!("Failed to create operation record: {}", e);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to create operation: {}", e),
                    )
                        .into_response();
                }

                // Spawn async task to execute the operation
                tokio::spawn(async move {
                    Self::execute_publish_operation(Arc::clone(&context), &req, operation_id).await
                });

                // Return operation ID immediately
                Json(PublishResponse::new(operation_id)).into_response()
            }
            Err(e) => {
                let error_messages: Vec<String> = e
                    .field_errors()
                    .iter()
                    .map(|(field, errors)| {
                        let messages: Vec<String> = errors
                            .iter()
                            .filter_map(|err| err.message.as_ref().map(|m| m.to_string()))
                            .collect();
                        format!("{}: {}", field, messages.join(", "))
                    })
                    .collect();

                (
                    StatusCode::BAD_REQUEST,
                    format!("Validation error: {}", error_messages.join("; ")),
                )
                    .into_response()
            }
        }
    }

    async fn execute_publish_operation(
        context: Arc<Context>,
        request: &PublishRequest,
        operation_id: OperationId,
    ) {
        // Destructure request early to take ownership and avoid clones later
        let PublishRequest {
            blockchain,
            dataset_root,
            dataset,
            minimum_number_of_node_replications,
            ..
        } = request;

        tracing::info!(
            "Starting publish operation - operation_id: {}, dataset_root: {}, blockchain: {}",
            operation_id,
            dataset_root,
            blockchain
        );

        if let Err(e) = context
            .pending_storage_service()
            .store_dataset(operation_id, dataset_root, dataset)
            .await
        {
            tracing::error!(
                "Failed to store dataset for operation {}: {}",
                operation_id,
                e
            );
            Self::handle_operation_failure(&context, operation_id, &e.to_string(), "store dataset")
                .await;

            return;
        }

        tracing::debug!(
            "Dataset stored successfully for operation {} (dataset_root: {})",
            operation_id,
            dataset_root
        );

        tracing::debug!(
            "Searching for shard for operation: {operation_id}, dataset root: {dataset_root}"
        );

        let shard_nodes = match context
            .repository_manager()
            .shard_repository()
            .get_all_peer_records(blockchain.as_str(), true)
            .await
        {
            Ok(shard_nodes) => shard_nodes,
            Err(e) => {
                let error_message = format!(
                    "Failed to get shard nodes from repository for operation: {operation_id}. Error: {e}"
                );

                if let Err(e) = context
                    .publish_service()
                    .mark_failed(operation_id, &error_message)
                    .await
                {
                    tracing::error!("Unable to mark operation {} as failed: {}", operation_id, e)
                }
                return;
            }
        };

        tracing::debug!(
            "Found {} node(s) for operation: {operation_id}",
            shard_nodes.len()
        );

        let min_ack_responses = minimum_number_of_node_replications
            .unwrap_or(context.publish_service().min_ack_responses());

        let peers: Vec<PeerId> = shard_nodes
            .iter()
            .map(|record| record.peer_id.parse().unwrap())
            .collect();

        let total_peers = peers.len() as u16;

        // Initialize progress tracking (operation record already created in handle_request)
        if let Err(e) = context
            .publish_service()
            .operation_manager()
            .initialize_progress(
                operation_id.into_inner(),
                total_peers,
                min_ack_responses as u16,
            )
            .await
        {
            tracing::error!("Failed to initialize progress for {operation_id}: {e}");
            let _ = context
                .publish_service()
                .mark_failed(operation_id, &e.to_string())
                .await;
            return;
        }

        if peers.len() < min_ack_responses as usize {
            let error_message = format!(
                "Unable to find enough nodes for operation: {operation_id}. Minimum number of nodes required: {min_ack_responses}"
            );

            // TODO: in js implementation the operation result data is removed. check how it's
            // handled when it's read by client
            if let Err(e) = context
                .publish_service()
                .mark_failed(operation_id, &error_message)
                .await
            {
                tracing::error!("Unable to mark operation {} as failed: {}", operation_id, e)
            }

            return;
        }

        let dataset = match context
            .pending_storage_service()
            .get_dataset(operation_id)
            .await
        {
            Ok(data) => data.dataset().clone(),
            Err(e) => {
                if let Err(e) = context
                    .publish_service()
                    .mark_failed(operation_id, &e.to_string())
                    .await
                {
                    tracing::error!("Unable to mark operation {} as failed: {}", operation_id, e)
                }

                return;
            }
        };

        let my_peer_id = *context.network_manager().peer_id();
        let mut send_futures = Vec::with_capacity(shard_nodes.len());

        let identity_id = context
            .blockchain_manager()
            .get_identity_id(blockchain)
            .await
            .unwrap()
            .unwrap();

        for node in shard_nodes {
            let remote_peer_id: PeerId = node.peer_id.parse().unwrap();

            if remote_peer_id == my_peer_id {
                let signature = context
                    .blockchain_manager()
                    .sign_message(blockchain, dataset_root.strip_prefix("0x").unwrap())
                    .await
                    .unwrap();
                let signature = blockchain::utils::split_signature(signature).unwrap();

                context
                    .repository_manager()
                    .signature_repository()
                    .create(
                        operation_id.into_inner(),
                        "network",
                        &identity_id.to_string(),
                        signature.v,
                        &signature.r,
                        &signature.s,
                        &signature.vs,
                    )
                    .await
                    .unwrap();
                context
                    .publish_service()
                    .operation_manager()
                    .record_response(operation_id.into_inner(), true)
                    .await
                    .unwrap();
            } else {
                let network_manager = Arc::clone(context.network_manager());
                let message = RequestMessage {
                    header: RequestMessageHeader {
                        operation_id: operation_id.into_inner(),
                        message_type: RequestMessageType::ProtocolRequest,
                    },
                    data: StoreRequestData::new(
                        dataset.public.clone(),
                        dataset_root.clone(),
                        blockchain.to_owned(),
                    ),
                };

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

        let dataset_root: H256 = dataset_root.parse().unwrap();
        let tokens = vec![
            Token::Uint(identity_id.into()),
            Token::FixedBytes(dataset_root.as_bytes().to_vec()),
        ];
        let message_hash = blockchain::utils::keccak256_encode_packed(&tokens).unwrap();
        let signature = context
            .blockchain_manager()
            .sign_message(
                blockchain,
                &format!(
                    "0x{}",
                    blockchain::utils::to_hex_string(message_hash.to_vec())
                ),
            )
            .await
            .unwrap();
        let signature = blockchain::utils::split_signature(signature).unwrap();

        context
            .repository_manager()
            .signature_repository()
            .create(
                operation_id.into_inner(),
                "publisher",
                &identity_id.to_string(),
                signature.v,
                &signature.r,
                &signature.s,
                &signature.vs,
            )
            .await
            .unwrap();
    }

    async fn handle_operation_failure(
        context: &Arc<Context>,
        operation_id: OperationId,
        error_message: &str,
        stage: &str,
    ) {
        if let Err(mark_error) = context
            .publish_service()
            .mark_failed(operation_id, error_message)
            .await
        {
            tracing::error!(
                "Unable to mark operation {} as failed (stage: {}): {}",
                operation_id,
                stage,
                mark_error
            );
        }
    }
}
