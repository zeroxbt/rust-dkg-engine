use crate::context::Context;
use crate::services::operation_service::OperationId;
use axum::Json;
use axum::{extract::State, response::IntoResponse};
use blockchain::BlockchainName;
use hyper::StatusCode;
use network::message::{RequestMessage, RequestMessageHeader, StoreRequestData};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use validator::Validate;
use validator_derive::Validate;

const DEFAULT_HASH_FUNCTION_ID: u8 = 1;

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
                let operation_id = OperationId::new();

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
        operation_id: OperationId,
    ) {
        tracing::info!(
            "Received asset with dataset root: {}, blockchain: {}",
            request.dataset_root,
            request.blockchain
        );

        let hash_function_id = request.hash_function_id.unwrap_or(DEFAULT_HASH_FUNCTION_ID);

        let request_message = RequestMessage {
            header: RequestMessageHeader {
                operation_id: operation_id.into_inner(),
                message_type: network::message::RequestMessageType::ProtocolRequest,
            },
            data: StoreRequestData::new(
                request.dataset.public,
                request.dataset_root,
                request.blockchain.as_str().to_string(),
            ),
        };

        /*  context
        .publish_service()
        .start_operation(
            operation_id,
            request.blockchain,
            keyword,
            hash_function_id,
            request_message,
        )
        .await; */
    }
}
