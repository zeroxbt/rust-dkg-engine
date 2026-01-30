#![allow(unreachable_pub)]

use std::str::FromStr;

use sea_orm::{
    entity::prelude::{DeriveRelation, EnumIter},
    prelude::{
        ActiveModelBehavior, DateTimeUtc, DeriveEntityModel, DerivePrimaryKey, PrimaryKeyTrait,
    },
};
use serde::{Deserialize, Serialize};

use crate::managers::repository::types::OperationStatus;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "operation_status")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub operation_id: String,
    pub operation_name: String,
    pub status: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub error_message: Option<String>,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

impl Model {
    /// Parse the status string into an OperationStatus enum.
    ///
    /// Returns an error if the status string is not a valid operation status.
    pub fn operation_status(&self) -> Result<OperationStatus, String> {
        OperationStatus::from_str(&self.status)
    }
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
