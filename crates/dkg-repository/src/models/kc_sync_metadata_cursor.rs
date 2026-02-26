#![allow(unreachable_pub)]

use sea_orm::{
    entity::prelude::{DeriveRelation, EnumIter},
    prelude::{ActiveModelBehavior, DeriveEntityModel, DerivePrimaryKey, PrimaryKeyTrait},
};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "kc_sync_metadata_cursor")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub blockchain_id: String,
    #[sea_orm(primary_key)]
    pub contract_address: String,
    pub last_checked_block: i64,
    pub updated_at: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
