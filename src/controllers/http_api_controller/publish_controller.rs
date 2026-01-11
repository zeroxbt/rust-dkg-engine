use crate::commands::command::CommandData;
use crate::commands::publish_replication_command::PublishReplicationCommandData;
use crate::context::Context;
use crate::services::operation_service::{OperationId, OperationLifecycle};
use axum::Json;
use axum::{extract::State, response::IntoResponse};
use blockchain::BlockchainName;
use hyper::StatusCode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use validator::Validate;
use validator_derive::Validate;

pub struct PublishController;

#[derive(Debug, Serialize, Deserialize, Validate)]
pub struct Assertion {
    #[validate(length(min = 1))]
    pub public: Vec<String>,
    pub private: Option<Vec<String>>,
}

#[derive(Deserialize, Debug, Validate)]
#[serde(rename_all = "camelCase")]
pub struct PublishRequest {
    #[validate(length(equal = 66))]
    pub dataset_root: String,
    pub dataset: Assertion,
    pub blockchain: BlockchainName,
    #[validate(range(min = 1))]
    pub hash_function_id: Option<u8>,
    #[validate(range(min = 1))]
    pub minimum_number_of_node_replications: Option<u8>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PublishResponse {
    operation_id: OperationId,
}

impl PublishController {
    pub async fn handle_request(
        State(context): State<Arc<Context>>,
        Json(req): Json<PublishRequest>,
    ) -> impl IntoResponse {
        match req.validate() {
            Ok(_) => {
                // Generate operation ID
                let operation_id = context
                    .publish_service()
                    .create_operation_record()
                    .await
                    .unwrap();

                // Spawn async task to execute the operation
                tokio::spawn(async move {
                    Self::execute_publish_operation(Arc::clone(&context), req, operation_id).await
                });

                // Return operation ID immediately
                Json(PublishResponse { operation_id }).into_response()
            }
            Err(e) => (
                StatusCode::BAD_REQUEST,
                format!("Validation error: {:?}", e),
            )
                .into_response(),
        }
    }

    async fn execute_publish_operation(
        context: Arc<Context>,
        request: PublishRequest,
        operation_id: OperationId,
    ) {
        tracing::info!(
            "Received asset with dataset root: {}, blockchain: {}",
            request.dataset_root,
            request.blockchain
        );

        let _peer_id = context.network_manager().peer_id();

        // TODO: store in pending storage before scheduling command

        let command = PublishReplicationCommandData::new(
            operation_id,
            request.blockchain.clone(),
            request.dataset_root.clone(),
            request.minimum_number_of_node_replications,
        )
        .into_command();

        let schedule_result = context.schedule_command_tx().send(command).await;

        if let Err(e) = schedule_result {
            let mark_result = context
                .publish_service()
                .mark_failed(operation_id, Box::new(e))
                .await;
            if let Err(e) = mark_result {
                tracing::error!(
                    "Unable to mark operation with id: {operation_id} as failed. Error: {e}"
                );
            }
        }
    }
}
