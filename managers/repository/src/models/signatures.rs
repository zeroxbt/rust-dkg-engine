use sea_orm::{
    EntityTrait, RelationTrait,
    entity::prelude::{DeriveRelation, EnumIter, Related},
    prelude::{
        ActiveModelBehavior, DateTimeUtc, DeriveEntityModel, DerivePrimaryKey, PrimaryKeyTrait,
    },
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::operations;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "signatures")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub operation_id: Uuid,
    pub is_publisher: bool,
    pub identity_id: String,
    pub v: u8,
    pub r: String,
    pub s: String,
    pub vs: String,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "operations::Entity",
        from = "Column::OperationId",
        to = "operations::Column::OperationId"
    )]
    Operation,
}

impl Related<operations::Entity> for Entity {
    fn to() -> sea_orm::RelationDef {
        Relation::Operation.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
