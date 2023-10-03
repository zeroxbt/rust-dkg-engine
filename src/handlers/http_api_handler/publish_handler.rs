use crate::commands::find_nodes_command::FindNodesCommand;
use crate::{commands::find_nodes_command::ProtocolOperation, context::Context};
use axum::Json;
use axum::{extract::State, response::IntoResponse};
use blockchain::BlockchainName;
use hyper::StatusCode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;
use validator::Validate;
use validator_derive::Validate;

pub struct PublishHandler;

#[derive(Deserialize, Debug, Validate)]
#[serde(rename_all = "camelCase")]
pub struct PublishRequest {
    #[validate(length(equal = 66))]
    pub assertion_id: String,

    #[validate(length(min = 1))]
    pub assertion: Vec<String>,

    pub blockchain: BlockchainName,

    #[validate(length(equal = 42))]
    pub contract: String,

    #[validate(range(min = 0))]
    pub token_id: u64,

    #[validate(range(min = 1))]
    pub hash_function_id: Option<i32>,
}

#[derive(Serialize, Debug, Clone)]
pub struct PublishResponse {
    operation_id: Uuid,
}

impl PublishHandler {
    pub async fn handle_request(
        State(context): State<Arc<Context>>,
        Json(req): Json<PublishRequest>,
    ) -> impl IntoResponse {
        match req.validate() {
            Ok(_) => {
                let operation_id = Uuid::new_v4();

                tokio::spawn(async move {
                    Self::execute_publish_operation(Arc::clone(&context), req, operation_id).await
                });

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
        operation_id: Uuid,
    ) {
        tracing::info!("Scheduling dial peers command...");
        context
            .schedule_command_tx()
            .send(Box::new(FindNodesCommand::new(
                operation_id,
                "".to_string(),
                request.blockchain.clone(),
                ProtocolOperation::Publish,
                request.hash_function_id.unwrap_or(1),
            )))
            .await
            .unwrap();

        tracing::debug!("received publish request: {:?}", request);
        tracing::info!("Finished scheduling dial peers command...");
    }
}
