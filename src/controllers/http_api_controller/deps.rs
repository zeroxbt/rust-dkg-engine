use std::sync::Arc;

use axum::extract::FromRef;
use dkg_repository::RepositoryManager;

use crate::{
    commands::scheduler::CommandScheduler,
    operations::{GetOperation, PublishStoreOperation},
    services::OperationStatusService,
};

#[derive(Clone)]
pub(crate) struct HttpApiDeps {
    pub(crate) command_scheduler: CommandScheduler,
    pub(crate) repository_manager: Arc<RepositoryManager>,
    pub(crate) get_operation_status_service: Arc<OperationStatusService<GetOperation>>,
    pub(crate) publish_store_operation_status_service:
        Arc<OperationStatusService<PublishStoreOperation>>,
}

#[derive(Clone)]
pub(crate) struct GetHttpApiControllerDeps {
    pub(crate) command_scheduler: CommandScheduler,
    pub(crate) get_operation_status_service: Arc<OperationStatusService<GetOperation>>,
}

impl FromRef<HttpApiDeps> for GetHttpApiControllerDeps {
    fn from_ref(input: &HttpApiDeps) -> Self {
        Self {
            command_scheduler: input.command_scheduler.clone(),
            get_operation_status_service: Arc::clone(&input.get_operation_status_service),
        }
    }
}

#[derive(Clone)]
pub(crate) struct PublishStoreHttpApiControllerDeps {
    pub(crate) command_scheduler: CommandScheduler,
    pub(crate) publish_store_operation_status_service:
        Arc<OperationStatusService<PublishStoreOperation>>,
}

impl FromRef<HttpApiDeps> for PublishStoreHttpApiControllerDeps {
    fn from_ref(input: &HttpApiDeps) -> Self {
        Self {
            command_scheduler: input.command_scheduler.clone(),
            publish_store_operation_status_service: Arc::clone(
                &input.publish_store_operation_status_service,
            ),
        }
    }
}

#[derive(Clone)]
pub(crate) struct PublishFinalityStatusHttpApiControllerDeps {
    pub(crate) repository_manager: Arc<RepositoryManager>,
}

impl FromRef<HttpApiDeps> for PublishFinalityStatusHttpApiControllerDeps {
    fn from_ref(input: &HttpApiDeps) -> Self {
        Self {
            repository_manager: Arc::clone(&input.repository_manager),
        }
    }
}

#[derive(Clone)]
pub(crate) struct OperationResultHttpApiControllerDeps {
    pub(crate) repository_manager: Arc<RepositoryManager>,
    pub(crate) get_operation_status_service: Arc<OperationStatusService<GetOperation>>,
    pub(crate) publish_store_operation_status_service:
        Arc<OperationStatusService<PublishStoreOperation>>,
}

impl FromRef<HttpApiDeps> for OperationResultHttpApiControllerDeps {
    fn from_ref(input: &HttpApiDeps) -> Self {
        Self {
            repository_manager: Arc::clone(&input.repository_manager),
            get_operation_status_service: Arc::clone(&input.get_operation_status_service),
            publish_store_operation_status_service: Arc::clone(
                &input.publish_store_operation_status_service,
            ),
        }
    }
}
