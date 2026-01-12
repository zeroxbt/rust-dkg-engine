use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::{Validate, ValidationError, ValidationErrors};
use validator_derive::Validate;

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
    pub operation_id: Uuid,
}

impl GetResponse {
    pub fn new(operation_id: Uuid) -> Self {
        Self { operation_id }
    }
}
