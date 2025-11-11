use crate::context::Context;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;
use validator::{Validate, ValidationError, ValidationErrors};
use validator_derive::Validate;

pub struct GetController;
#[derive(Debug, Deserialize)]
pub enum StateField {
    StateEnum(GetStates),
    AssertionId(String),
}

impl Validate for StateField {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            StateField::AssertionId(s) if s.starts_with("0x") && s.len() == 66 => Ok(()),
            StateField::StateEnum(_) => Ok(()),
            _ => {
                let mut errors = ValidationErrors::new();
                let error = ValidationError::new("invalid_state");
                errors.add("state", error);
                Err(errors)
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum GetStates {
    #[serde(rename = "LATEST")]
    Latest,
    #[serde(rename = "FINALIZED")]
    Finalized,
}

#[derive(Deserialize, Debug, Validate)]
#[serde(rename_all = "camelCase")]
pub struct GetRequest {
    pub id: String,

    #[validate]
    pub state: Option<StateField>,

    #[validate(range(min = 1))]
    pub hash_function_id: Option<u8>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetResponse {
    operation_id: Uuid,
}

impl GetController {
    pub async fn handle_request(
        State(context): State<Arc<Context>>,
        Json(req): Json<GetRequest>,
    ) -> impl IntoResponse {
        match req.validate() {
            Ok(_) => {
                let operation_id = Uuid::new_v4();

                tokio::spawn(async move {
                    Self::execute_get_operation(Arc::clone(&context), req, operation_id).await
                });

                Json(GetResponse { operation_id }).into_response()
            }
            Err(e) => (
                StatusCode::BAD_REQUEST,
                format!("Validation error: {:?}", e),
            )
                .into_response(),
        }
    }

    async fn execute_get_operation(context: Arc<Context>, request: GetRequest, operation_id: Uuid) {
    }
}
