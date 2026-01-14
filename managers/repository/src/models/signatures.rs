use sea_orm::{
    entity::prelude::{DeriveRelation, EnumIter},
    prelude::{
        ActiveModelBehavior, DateTimeUtc, DeriveEntityModel, DerivePrimaryKey, PrimaryKeyTrait,
    },
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "signatures")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub operation_id: String,
    pub signature_type: String,
    pub identity_id: String,
    pub v: u8,
    pub r: String,
    pub s: String,
    pub vs: String,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
