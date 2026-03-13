use std::sync::Arc;

use dkg_key_value_store::PublishTmpDatasetStore;
use dkg_repository::{FinalityStatusRepository, OperationRepository, ProofChallengeRepository};

use crate::{
    application::OperationTracking,
    operations::{GetOperation, PublishStoreOperation},
};

#[derive(Clone)]
pub(crate) struct CleanupDeps {
    pub(crate) operation_repository: OperationRepository,
    pub(crate) finality_status_repository: FinalityStatusRepository,
    pub(crate) proof_challenge_repository: ProofChallengeRepository,
    pub(crate) publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
    pub(crate) publish_operation_tracking: Arc<OperationTracking<PublishStoreOperation>>,
    pub(crate) get_operation_tracking: Arc<OperationTracking<GetOperation>>,
}
