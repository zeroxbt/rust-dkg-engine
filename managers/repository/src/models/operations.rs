use sea_orm::{
    EntityTrait, RelationTrait,
    entity::prelude::{DeriveRelation, EnumIter, Related},
    prelude::{
        ActiveModelBehavior, DateTimeUtc, DeriveEntityModel, DerivePrimaryKey, PrimaryKeyTrait,
    },
};

use super::signatures;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "operations")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub operation_id: Uuid,
    pub operation_name: String,
    pub status: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub error_message: Option<String>,
    pub timestamp: i64,
    // Progress tracking fields
    pub total_peers: Option<u16>,
    pub min_ack_responses: Option<u16>,
    pub completed_count: u16,
    pub failed_count: u16,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "signatures::Entity")]
    Signatures,
}

impl Related<signatures::Entity> for Entity {
    fn to() -> sea_orm::RelationDef {
        Relation::Signatures.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
