use sea_orm::{
    entity::prelude::{DeriveRelation, EnumIter},
    prelude::{
        ActiveModelBehavior, DateTimeUtc, DeriveEntityModel, DerivePrimaryKey, EntityTrait,
        PrimaryKeyTrait,
    },
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "operation_ids")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub operation_id: Uuid,
    #[sea_orm(column_type = "Text", nullable)]
    pub data: Option<String>,
    pub status: String,
    pub timestamp: i64,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
