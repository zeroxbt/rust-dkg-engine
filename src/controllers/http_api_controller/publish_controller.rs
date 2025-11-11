use crate::{context::Context, services::operation_service::OperationService};
use axum::Json;
use axum::{extract::State, response::IntoResponse};
use blockchain::{Address, BlockchainName};
use hyper::StatusCode;
use network::message::{
    RequestMessage, RequestMessageHeader, StoreInitRequestData, StoreMessageRequestData,
    StoreRequestData,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;
use validator::Validate;
use validator_derive::Validate;

const DEFAULT_HASH_FUNCTION_ID: u8 = 1;

pub struct PublishController;

#[derive(Deserialize, Debug, Validate)]
#[serde(rename_all = "camelCase")]
pub struct PublishRequest {
    #[validate(length(equal = 66))]
    pub assertion_id: String,

    #[validate(length(min = 1))]
    pub assertion: Vec<String>,

    pub blockchain: BlockchainName,

    pub contract: Address,

    #[validate(range(min = 0))]
    pub token_id: u64,

    #[validate(range(min = 1))]
    pub hash_function_id: Option<u8>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PublishResponse {
    operation_id: Uuid,
}

impl PublishController {
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
        let keyword = context
            .ual_service()
            .calculate_location_keyword(&request.blockchain, &request.contract, request.token_id)
            .await;

        let hash_function_id = request.hash_function_id.unwrap_or(DEFAULT_HASH_FUNCTION_ID);

        let init_message = RequestMessage {
            header: RequestMessageHeader {
                operation_id,
                keyword_uuid: Uuid::new_v5(&Uuid::NAMESPACE_URL, &keyword),
                message_type: network::message::RequestMessageType::ProtocolInit,
            },
            data: StoreMessageRequestData::Init(StoreInitRequestData::new(
                request.assertion_id,
                request.blockchain.as_str().to_string(),
                request.contract.to_string(),
                request.token_id,
                blockchain::utils::to_hex_string(keyword.clone()),
                hash_function_id,
            )),
        };

        let request_message = RequestMessage {
            header: RequestMessageHeader {
                operation_id,
                keyword_uuid: Uuid::new_v5(&Uuid::NAMESPACE_URL, &keyword),
                message_type: network::message::RequestMessageType::ProtocolRequest,
            },
            data: StoreMessageRequestData::Request(StoreRequestData::new(request.assertion)),
        };

        context
            .publish_service()
            .start_operation(
                operation_id,
                request.blockchain,
                keyword,
                hash_function_id,
                init_message,
                request_message,
            )
            .await;
    }
}
