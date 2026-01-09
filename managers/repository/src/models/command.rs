use sea_orm::{
    entity::prelude::{DeriveRelation, EnumIter, Json},
    prelude::{
        ActiveModelBehavior, DateTimeUtc, DeriveEntityModel, DerivePrimaryKey,
        PrimaryKeyTrait,
    },
};
use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Deserialize)]
#[sea_orm(table_name = "command")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: String,
    pub name: String,
    pub data: Json,
    pub sequence: Json,
    pub ready_at: i64,
    pub delay: i64,
    pub started_at: Option<i64>,
    pub deadline_at: Option<i64>,
    pub period: Option<i64>,
    pub status: String,
    #[sea_orm(column_type = "Text")]
    pub message: Option<String>,
    pub parent_id: Option<String>,
    pub retries: i32,
    pub transactional: bool,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
