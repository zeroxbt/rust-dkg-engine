use sea_orm::{
    entity::prelude::{DeriveRelation, EnumIter},
    prelude::{
        ActiveModelBehavior, DateTimeUtc, DeriveEntityModel, DerivePrimaryKey,
        PrimaryKeyTrait,
    },
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "operation_responses")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub operation_id: Uuid,
    pub node_id: Option<String>,
    pub status: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub message: Option<String>,
    pub keyword: Option<String>,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
