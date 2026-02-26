#![allow(unreachable_pub)]

use sea_orm::{
    entity::prelude::{DeriveRelation, EnumIter},
    prelude::{ActiveModelBehavior, DeriveEntityModel, DerivePrimaryKey, PrimaryKeyTrait},
};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "kc_chain_state_metadata")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub blockchain_id: String,
    #[sea_orm(primary_key)]
    pub contract_address: String,
    #[sea_orm(primary_key)]
    pub kc_id: u64,
    pub range_start_token_id: u32,
    pub range_end_token_id: u32,
    pub burned_mode: u32,
    pub burned_payload: Vec<u8>,
    pub end_epoch: i64,
    pub latest_merkle_root: String,
    pub state_observed_block: i64,
    pub state_updated_at: i64,
    pub private_graph_mode: Option<u32>,
    pub private_graph_payload: Option<Vec<u8>>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
