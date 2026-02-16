#![allow(unreachable_pub)]

use sea_orm::{
    entity::prelude::{DeriveRelation, EnumIter},
    prelude::{ActiveModelBehavior, DeriveEntityModel, DerivePrimaryKey, PrimaryKeyTrait},
};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "proof_challenge")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub blockchain_id: String,
    #[sea_orm(primary_key)]
    pub epoch: i64,
    #[sea_orm(primary_key)]
    pub proof_period_start_block: i64,
    pub contract_address: String,
    pub knowledge_collection_id: i64,
    pub chunk_index: i64,
    pub state: String,
    pub score: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
