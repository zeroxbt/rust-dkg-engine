use std::sync::Arc;

use chrono::Utc;
use sea_orm::{
    ActiveValue, ColumnTrait, Condition, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect,
};

use crate::managers::repository::{
    error::Result,
    models::proof_challenge::{ActiveModel, Column, Entity, Model},
};

/// Challenge state for the proofing workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChallengeState {
    /// Challenge created, proof not yet submitted
    Pending,
    /// Proof tx sent, awaiting on-chain confirmation
    Submitted,
    /// Confirmed with score > 0
    Finalized,
}

impl ChallengeState {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Submitted => "submitted",
            Self::Finalized => "finalized",
        }
    }

    pub(crate) fn from_str(s: &str) -> Self {
        match s {
            "submitted" => Self::Submitted,
            "finalized" => Self::Finalized,
            _ => Self::Pending,
        }
    }
}

pub(crate) struct ProofChallengeRepository {
    conn: Arc<DatabaseConnection>,
}

impl ProofChallengeRepository {
    pub(crate) fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    /// Get the latest challenge for a blockchain (by creation time).
    pub(crate) async fn get_latest(&self, blockchain_id: &str) -> Result<Option<Model>> {
        Ok(Entity::find()
            .filter(Column::BlockchainId.eq(blockchain_id))
            .order_by_desc(Column::CreatedAt)
            .one(self.conn.as_ref())
            .await?)
    }

    /// Create a new challenge record.
    pub(crate) async fn create(
        &self,
        blockchain_id: &str,
        epoch: i64,
        proof_period_start_block: i64,
        contract_address: &str,
        knowledge_collection_id: i64,
        chunk_index: i64,
    ) -> Result<Model> {
        let now = Utc::now().timestamp();

        let model = ActiveModel {
            blockchain_id: ActiveValue::Set(blockchain_id.to_string()),
            epoch: ActiveValue::Set(epoch),
            proof_period_start_block: ActiveValue::Set(proof_period_start_block),
            contract_address: ActiveValue::Set(contract_address.to_string()),
            knowledge_collection_id: ActiveValue::Set(knowledge_collection_id),
            chunk_index: ActiveValue::Set(chunk_index),
            state: ActiveValue::Set(ChallengeState::Pending.as_str().to_string()),
            score: ActiveValue::Set(None),
            created_at: ActiveValue::Set(now),
            updated_at: ActiveValue::Set(now),
        };

        Entity::insert(model).exec(self.conn.as_ref()).await?;

        // Fetch the inserted record
        Entity::find()
            .filter(Column::BlockchainId.eq(blockchain_id))
            .filter(Column::Epoch.eq(epoch))
            .filter(Column::ProofPeriodStartBlock.eq(proof_period_start_block))
            .one(self.conn.as_ref())
            .await?
            .ok_or_else(|| {
                crate::managers::repository::error::RepositoryError::NotFound(format!(
                    "Challenge {}:{}:{}",
                    blockchain_id, epoch, proof_period_start_block
                ))
            })
    }

    /// Update the state of a challenge.
    pub(crate) async fn set_state(
        &self,
        blockchain_id: &str,
        epoch: i64,
        proof_period_start_block: i64,
        state: ChallengeState,
        score: Option<String>,
    ) -> Result<()> {
        let now = Utc::now().timestamp();

        // Find the existing record
        let existing = Entity::find()
            .filter(Column::BlockchainId.eq(blockchain_id))
            .filter(Column::Epoch.eq(epoch))
            .filter(Column::ProofPeriodStartBlock.eq(proof_period_start_block))
            .one(self.conn.as_ref())
            .await?;

        let Some(existing) = existing else {
            return Ok(());
        };

        let update = ActiveModel {
            blockchain_id: ActiveValue::Unchanged(existing.blockchain_id),
            epoch: ActiveValue::Unchanged(existing.epoch),
            proof_period_start_block: ActiveValue::Unchanged(existing.proof_period_start_block),
            contract_address: ActiveValue::Unchanged(existing.contract_address),
            knowledge_collection_id: ActiveValue::Unchanged(existing.knowledge_collection_id),
            chunk_index: ActiveValue::Unchanged(existing.chunk_index),
            state: ActiveValue::Set(state.as_str().to_string()),
            score: ActiveValue::Set(score),
            created_at: ActiveValue::Unchanged(existing.created_at),
            updated_at: ActiveValue::Set(now),
        };

        Entity::update(update).exec(self.conn.as_ref()).await?;

        Ok(())
    }

    /// Find challenges older than cutoff (based on updated_at).
    pub(crate) async fn find_expired(&self, cutoff: i64, limit: u64) -> Result<Vec<Model>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        Ok(Entity::find()
            .filter(Column::UpdatedAt.lt(cutoff))
            .order_by_asc(Column::UpdatedAt)
            .limit(limit)
            .all(self.conn.as_ref())
            .await?)
    }

    /// Delete challenges by composite key. Returns rows affected.
    pub(crate) async fn delete_by_keys(&self, entries: &[Model]) -> Result<u64> {
        if entries.is_empty() {
            return Ok(0);
        }

        let mut condition = Condition::any();
        for entry in entries {
            condition = condition.add(
                Condition::all()
                    .add(Column::BlockchainId.eq(entry.blockchain_id.clone()))
                    .add(Column::Epoch.eq(entry.epoch))
                    .add(Column::ProofPeriodStartBlock.eq(entry.proof_period_start_block)),
            );
        }

        let result = Entity::delete_many()
            .filter(condition)
            .exec(self.conn.as_ref())
            .await?;

        Ok(result.rows_affected)
    }
}
