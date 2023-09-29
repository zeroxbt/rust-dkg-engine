use sea_orm::{
    entity::prelude::{DeriveRelation, EnumIter},
    prelude::{
        ActiveModelBehavior, DateTimeUtc, DeriveEntityModel, DerivePrimaryKey, EntityTrait,
        PrimaryKeyTrait,
    },
};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "shard")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub peer_id: String,
    #[sea_orm(primary_key)]
    pub blockchain_id: String,
    pub ask: String,
    pub stake: String,
    #[sea_orm(default_value = "1970-01-01 00:00:00")]
    pub last_seen: Option<DateTimeUtc>,
    #[sea_orm(default_value = "1970-01-01 00:00:00")]
    pub last_dialed: Option<DateTimeUtc>,
    pub sha256: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
