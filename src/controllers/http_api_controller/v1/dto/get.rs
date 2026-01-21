use serde::Serialize;
use triple_store::Visibility;
use uuid::Uuid;
use validator_derive::Validate;

#[derive(serde::Deserialize, Debug, Validate)]
#[serde(rename_all = "camelCase")]
pub struct GetRequest {
    /// The UAL (Universal Asset Locator) of the knowledge asset/collection to retrieve
    pub id: String,

    /// Whether to include metadata in the response
    #[serde(default)]
    pub include_metadata: bool,

    /// Optional paranet UAL for permissioned access
    #[serde(rename = "paranetUAL")]
    pub paranet_ual: Option<String>,

    /// Visibility filter: public, private, or all
    #[serde(default)]
    pub content_type: Visibility,
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
