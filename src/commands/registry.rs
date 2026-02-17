use std::sync::Arc;

use super::operations::{
    batch_get::handle_batch_get_request::{
        HandleBatchGetRequestCommandData, HandleBatchGetRequestCommandHandler,
    },
    get::{
        handle_get_request::{HandleGetRequestCommandData, HandleGetRequestCommandHandler},
        send_get_requests::{SendGetRequestsCommandData, SendGetRequestsCommandHandler},
    },
    publish::{
        finality::{
            handle_publish_finality_request::{
                HandlePublishFinalityRequestCommandData, HandlePublishFinalityRequestCommandHandler,
            },
            send_publish_finality_request::{
                SendPublishFinalityRequestCommandData, SendPublishFinalityRequestCommandHandler,
            },
        },
        store::{
            handle_publish_store_request::{
                HandlePublishStoreRequestCommandData, HandlePublishStoreRequestCommandHandler,
            },
            send_publish_store_requests::{
                SendPublishStoreRequestsCommandData, SendPublishStoreRequestsCommandHandler,
            },
        },
    },
};
use crate::commands::{
    HandleBatchGetRequestDeps, HandleGetRequestDeps, HandlePublishFinalityRequestDeps,
    HandlePublishStoreRequestDeps, SendGetRequestsDeps, SendPublishFinalityRequestDeps,
    SendPublishStoreRequestsDeps, executor::CommandOutcome,
};

macro_rules! command_registry {
    (
        $(
            $field:ident: $variant:ident => {
                data: $data:ty,
                handler: $handler:ty,
                deps: $deps_ty:ty
            }
        ),+ $(,)?
    ) => {
        #[derive(Clone)]
        pub(crate) enum Command {
            $( $variant($data), )+
        }

        impl Command {
            pub(crate) fn name(&self) -> &'static str {
                match self {
                    $( Self::$variant(_) => stringify!($variant), )+
                }
            }
        }

        $(
            impl From<$data> for Command {
                fn from(data: $data) -> Self {
                    Self::$variant(data)
                }
            }
        )+

        pub(crate) struct CommandResolver {
            $( $field: Arc<$handler>, )+
        }

        pub(crate) struct CommandResolverDeps {
            $( pub(crate) $field: $deps_ty, )+
        }

        impl CommandResolver {
            pub(crate) fn new(deps: CommandResolverDeps) -> Self {
                Self {
                    $( $field: Arc::new(<$handler>::new(deps.$field)), )+
                }
            }

            pub(crate) async fn execute(&self, command: &Command) -> CommandOutcome {
                match command {
                    $( Command::$variant(data) => self.$field.execute(data).await, )+
                }
            }
        }
    };
}

pub(crate) trait CommandHandler<D: Send + Sync + 'static>: Send + Sync {
    async fn execute(&self, data: &D) -> CommandOutcome;
}

// Command registry: operation commands only.
// Periodic tasks are managed separately in src/periodic/.
command_registry! {
    send_publish_store_requests: SendPublishStoreRequests => {
        data: SendPublishStoreRequestsCommandData,
        handler: SendPublishStoreRequestsCommandHandler,
        deps: SendPublishStoreRequestsDeps
    },
    handle_publish_store_request: HandlePublishStoreRequest => {
        data: HandlePublishStoreRequestCommandData,
        handler: HandlePublishStoreRequestCommandHandler,
        deps: HandlePublishStoreRequestDeps
    },
    send_publish_finality_request: SendPublishFinalityRequest => {
        data: SendPublishFinalityRequestCommandData,
        handler: SendPublishFinalityRequestCommandHandler,
        deps: SendPublishFinalityRequestDeps
    },
    handle_publish_finality_request: HandlePublishFinalityRequest => {
        data: HandlePublishFinalityRequestCommandData,
        handler: HandlePublishFinalityRequestCommandHandler,
        deps: HandlePublishFinalityRequestDeps
    },
    send_get_requests: SendGetRequests => {
        data: SendGetRequestsCommandData,
        handler: SendGetRequestsCommandHandler,
        deps: SendGetRequestsDeps
    },
    handle_get_request: HandleGetRequest => {
        data: HandleGetRequestCommandData,
        handler: HandleGetRequestCommandHandler,
        deps: HandleGetRequestDeps
    },
    handle_batch_get_request: HandleBatchGetRequest => {
        data: HandleBatchGetRequestCommandData,
        handler: HandleBatchGetRequestCommandHandler,
        deps: HandleBatchGetRequestDeps
    },
}
