#![allow(unreachable_pub)]

use sea_orm::{
    entity::prelude::{DeriveRelation, EnumIter},
    prelude::{ActiveModelBehavior, DeriveEntityModel, DerivePrimaryKey, PrimaryKeyTrait},
};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "kc_sync_queue")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub blockchain_id: String,
    #[sea_orm(primary_key)]
    pub contract_address: String,
    #[sea_orm(primary_key)]
    pub kc_id: u64,
    pub retry_count: u32,
    pub next_retry_at: i64,
    pub created_at: i64,
    pub last_retry_at: Option<i64>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
