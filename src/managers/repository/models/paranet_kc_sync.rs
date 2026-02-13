#![allow(unreachable_pub)]

use sea_orm::{
    entity::prelude::{DeriveRelation, EnumIter},
    prelude::{ActiveModelBehavior, DeriveEntityModel, DerivePrimaryKey, PrimaryKeyTrait},
};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "paranet_kc_sync")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub paranet_ual: String,
    #[sea_orm(primary_key)]
    pub kc_ual: String,
    pub blockchain_id: String,
    pub paranet_id: String,
    pub retry_count: u32,
    pub next_retry_at: i64,
    pub last_error: Option<String>,
    pub status: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
