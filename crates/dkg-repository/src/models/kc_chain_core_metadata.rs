#![allow(unreachable_pub)]

use sea_orm::{
    entity::prelude::{DeriveRelation, EnumIter},
    prelude::{ActiveModelBehavior, DeriveEntityModel, DerivePrimaryKey, PrimaryKeyTrait},
};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "kc_chain_core_metadata")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub blockchain_id: String,
    #[sea_orm(primary_key)]
    pub contract_address: String,
    #[sea_orm(primary_key)]
    pub kc_id: u64,
    pub publisher_address: Option<String>,
    pub block_number: i64,
    pub transaction_hash: String,
    pub block_timestamp: i64,
    pub publish_operation_id: String,
    pub source: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
