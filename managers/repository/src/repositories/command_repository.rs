use crate::models::commands::{self, Entity as CommandEntity, Model as CommandModel};
use sea_orm::entity::prelude::*;
use sea_orm::{error::DbErr, DatabaseConnection};
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter};
use std::sync::Arc;

pub struct CommandRepository {
    conn: Arc<DatabaseConnection>,
}

impl CommandRepository {
    pub fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    pub async fn update_command(
        &self,
        command: &CommandModel,
        new_status: Option<String>,
        new_started_at: Option<i64>,
        new_retries: Option<i32>,
    ) -> Result<(), DbErr> {
        let mut active_model: commands::ActiveModel = command.to_owned().into();

        if let Some(status) = new_status {
            active_model.set(commands::Column::Status, status.into());
        };
        if let Some(started_at) = new_started_at {
            active_model.set(commands::Column::StartedAt, started_at.into());
        };
        if let Some(retries) = new_retries {
            active_model.set(commands::Column::Retries, retries.into());
        };

        CommandEntity::update(active_model)
            .exec(self.conn.as_ref())
            .await?;

        Ok(())
    }

    pub async fn create_command(&self, command: &CommandModel) -> Result<(), DbErr> {
        let active_model: commands::ActiveModel = command.to_owned().into();

        CommandEntity::insert(active_model)
            .exec_without_returning(self.conn.as_ref())
            .await?;

        Ok(())
    }

    pub async fn get_command_with_id(&self, id: Uuid) -> Result<Option<CommandModel>, DbErr> {
        CommandEntity::find_by_id(id.hyphenated().to_string())
            .one(self.conn.as_ref())
            .await
    }

    pub async fn destroy_command(&self, name: &str) -> Result<(), DbErr> {
        let _ = CommandEntity::delete_many()
            .filter(commands::Column::Name.eq(name))
            .exec(self.conn.as_ref())
            .await?;
        Ok(())
    }

    pub async fn get_commands_with_status(
        &self,
        status_array: Vec<String>,
    ) -> Result<Vec<CommandModel>, DbErr> {
        CommandEntity::find()
            .filter(commands::Column::Status.is_in(status_array))
            .all(self.conn.as_ref())
            .await
    }

    /* pub async fn find_finalized_commands(
        &self,
        timestamp: i64,
        limit: i32,
    ) -> Result<Vec<CommandModel>, DbErr> {
        let statuses = [
            CommandStatus::Completed.to_string(),
            CommandStatus::Failed.to_string(),
            CommandStatus::Expired.to_string(),
            CommandStatus::Unknown.to_string(),
        ];

        CommandEntity::find(&self.conn)
            .filter(
                CommandEntity::Column::Status
                    .in_iter(statuses.iter().cloned())
                    .and(CommandEntity::Column::StartedAt.le(timestamp)),
            )
            .limit(limit)
            .all()
            .await
    } */
}
