use std::sync::Arc;

use sea_orm::{
    ActiveModelTrait, DatabaseConnection, EntityTrait, QuerySelect, Set, TransactionTrait,
};

use crate::managers::repository::{
    error::RepositoryError,
    models::triples_insert_count::{self, Entity, Model},
};

pub(crate) struct TriplesInsertCountRepository {
    conn: Arc<DatabaseConnection>,
}

impl TriplesInsertCountRepository {
    pub(crate) fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    /// Atomically increment the inserted triples count.
    /// Creates the record if it doesn't exist, otherwise increments atomically.
    pub(crate) async fn atomic_increment(&self, by: i64) -> Result<Model, RepositoryError> {
        let txn = self.conn.begin().await?;

        // Try to find existing record with exclusive lock
        let existing = Entity::find().lock_exclusive().one(&txn).await?;

        let result = if let Some(record) = existing {
            let mut active_model: triples_insert_count::ActiveModel = record.into();
            active_model.count = Set(active_model.count.unwrap() + by);
            active_model.update(&txn).await?
        } else {
            // No record exists, create one
            let active_model = triples_insert_count::ActiveModel {
                id: Set(1),
                count: Set(by),
            };
            Entity::insert(active_model)
                .exec_with_returning(&txn)
                .await?
        };

        txn.commit().await?;

        Ok(result)
    }
}
