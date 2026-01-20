use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator_derive::Validate;

#[derive(Debug, Deserialize)]
#[serde(untagged, rename_all = "lowercase")]
pub enum GetContentTypes {
    Public,
    Private,
    All,
}

#[derive(Deserialize, Debug, Validate)]
#[serde(rename_all = "camelCase")]
pub struct GetRequest {
    pub id: String,

    pub include_metadata: bool,

    #[serde(rename = "paranetUAL")]
    pub paranet_ual: Option<String>,

    content_type: GetContentTypes,
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
