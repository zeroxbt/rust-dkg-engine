use sea_orm::{
    entity::prelude::{DeriveRelation, EnumIter},
    prelude::{
        ActiveModelBehavior, DateTimeUtc, DeriveEntityModel, DerivePrimaryKey, EntityTrait,
        PrimaryKeyTrait,
    },
};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "blockchain")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub blockchain_id: String,
    #[sea_orm(primary_key)]
    pub contract: String,
    #[sea_orm(default_value = "0")]
    pub last_checked_block: u64,
    #[sea_orm(default_value = "1970-01-01 00:00:00")]
    pub last_checked_timestamp: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
