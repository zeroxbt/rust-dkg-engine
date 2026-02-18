use std::sync::Arc;

use axum::extract::FromRef;
use dkg_repository::{FinalityStatusRepository, OperationRepository};

use crate::{
    application::OperationTracking,
    commands::scheduler::CommandScheduler,
    operations::{GetOperation, PublishStoreOperation},
};

#[derive(Clone)]
pub(crate) struct HttpApiDeps {
    pub(crate) command_scheduler: CommandScheduler,
    pub(crate) operation_repository: OperationRepository,
    pub(crate) finality_status_repository: FinalityStatusRepository,
    pub(crate) get_operation_tracking: Arc<OperationTracking<GetOperation>>,
    pub(crate) publish_store_operation_tracking: Arc<OperationTracking<PublishStoreOperation>>,
}

#[derive(Clone)]
pub(crate) struct GetHttpApiControllerDeps {
    pub(crate) command_scheduler: CommandScheduler,
    pub(crate) get_operation_tracking: Arc<OperationTracking<GetOperation>>,
}

impl FromRef<HttpApiDeps> for GetHttpApiControllerDeps {
    fn from_ref(input: &HttpApiDeps) -> Self {
        Self {
            command_scheduler: input.command_scheduler.clone(),
            get_operation_tracking: Arc::clone(&input.get_operation_tracking),
        }
    }
}

#[derive(Clone)]
pub(crate) struct PublishStoreHttpApiControllerDeps {
    pub(crate) command_scheduler: CommandScheduler,
    pub(crate) publish_store_operation_tracking: Arc<OperationTracking<PublishStoreOperation>>,
}

impl FromRef<HttpApiDeps> for PublishStoreHttpApiControllerDeps {
    fn from_ref(input: &HttpApiDeps) -> Self {
        Self {
            command_scheduler: input.command_scheduler.clone(),
            publish_store_operation_tracking: Arc::clone(&input.publish_store_operation_tracking),
        }
    }
}

#[derive(Clone)]
pub(crate) struct PublishFinalityStatusHttpApiControllerDeps {
    pub(crate) finality_status_repository: FinalityStatusRepository,
}

impl FromRef<HttpApiDeps> for PublishFinalityStatusHttpApiControllerDeps {
    fn from_ref(input: &HttpApiDeps) -> Self {
        Self {
            finality_status_repository: input.finality_status_repository.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct OperationResultHttpApiControllerDeps {
    pub(crate) operation_repository: OperationRepository,
    pub(crate) get_operation_tracking: Arc<OperationTracking<GetOperation>>,
    pub(crate) publish_store_operation_tracking: Arc<OperationTracking<PublishStoreOperation>>,
}

impl FromRef<HttpApiDeps> for OperationResultHttpApiControllerDeps {
    fn from_ref(input: &HttpApiDeps) -> Self {
        Self {
            operation_repository: input.operation_repository.clone(),
            get_operation_tracking: Arc::clone(&input.get_operation_tracking),
            publish_store_operation_tracking: Arc::clone(&input.publish_store_operation_tracking),
        }
    }
}
