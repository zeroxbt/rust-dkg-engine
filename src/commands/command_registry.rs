use std::sync::Arc;

use super::{
    command_executor::CommandExecutionRequest,
    operations::{
        get::{
            handle_get_request::{HandleGetRequestCommandData, HandleGetRequestCommandHandler},
            send_get_requests::{SendGetRequestsCommandData, SendGetRequestsCommandHandler},
        },
        publish::{
            finality::{
                handle_publish_finality_request::{
                    HandlePublishFinalityRequestCommandData,
                    HandlePublishFinalityRequestCommandHandler,
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
    },
    periodic::{
        blockchain_event_listener::{
            BlockchainEventListenerCommandData, BlockchainEventListenerCommandHandler,
        },
        dial_peers::{DialPeersCommandData, DialPeersCommandHandler},
        sharding_table_check::{ShardingTableCheckCommandData, ShardingTableCheckCommandHandler},
        sync::{SyncCommandData, SyncCommandHandler},
    },
};
use crate::{
    commands::{
        command_executor::CommandExecutionResult,
        operations::batch_get::handle_batch_get_request::{
            HandleBatchGetRequestCommandData, HandleBatchGetRequestCommandHandler,
        },
    },
    context::Context,
    managers::blockchain::BlockchainId,
};

macro_rules! command_registry {
    (
        $(
            $field:ident: $variant:ident => {
                data: $data:ty,
                handler: $handler:ty
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

        impl CommandResolver {
            pub(crate) fn new(context: Arc<Context>) -> Self {
                Self {
                    $( $field: Arc::new(<$handler>::new(Arc::clone(&context))), )+
                }
            }

            pub(crate) async fn   execute(&self, command: &Command) -> CommandExecutionResult {
                match command {
                    $( Command::$variant(data) => self.$field.execute(data).await, )+
                }
            }

        }
    };
}

pub(crate) trait CommandHandler<D: Send + Sync + 'static>: Send + Sync {
    async fn execute(&self, data: &D) -> CommandExecutionResult;
}

// Command registry usage:
// - Add one entry per command with a data payload type and handler type.
// - Default scheduling is declared explicitly below for clarity.
command_registry! {
    blockchain_event_listener: BlockchainEventListener => {
        data: BlockchainEventListenerCommandData,
        handler: BlockchainEventListenerCommandHandler
    },
    dial_peers: DialPeers => {
        data: DialPeersCommandData,
        handler: DialPeersCommandHandler
    },
    sharding_table_check: ShardingTableCheck => {
        data: ShardingTableCheckCommandData,
        handler: ShardingTableCheckCommandHandler
    },
    send_publish_store_requests: SendPublishStoreRequests => {
        data: SendPublishStoreRequestsCommandData,
        handler: SendPublishStoreRequestsCommandHandler
    },
    handle_publish_store_request: HandlePublishStoreRequest => {
        data: HandlePublishStoreRequestCommandData,
        handler: HandlePublishStoreRequestCommandHandler
    },
    send_publish_finality_request: SendPublishFinalityRequest => {
        data: SendPublishFinalityRequestCommandData,
        handler: SendPublishFinalityRequestCommandHandler
    },
    handle_publish_finality_request: HandlePublishFinalityRequest => {
        data: HandlePublishFinalityRequestCommandData,
        handler: HandlePublishFinalityRequestCommandHandler
    },
    send_get_requests: SendGetRequests => {
        data: SendGetRequestsCommandData,
        handler: SendGetRequestsCommandHandler
    },
    handle_get_request: HandleGetRequest => {
        data: HandleGetRequestCommandData,
        handler: HandleGetRequestCommandHandler
    },
    handle_batch_get_request: HandleBatchGetRequest => {
        data: HandleBatchGetRequestCommandData,
        handler: HandleBatchGetRequestCommandHandler
    },
    sync: Sync => {
        data: SyncCommandData,
        handler: SyncCommandHandler
    }
}

/// Default commands scheduled at startup. Keep this list explicit for clarity.
pub(crate) fn default_command_requests(
    blockchain_ids: &[BlockchainId],
) -> Vec<CommandExecutionRequest> {
    let mut requests = vec![
        CommandExecutionRequest::new(Command::DialPeers(DialPeersCommandData)),
        CommandExecutionRequest::new(Command::ShardingTableCheck(ShardingTableCheckCommandData)),
    ];

    // Schedule one blockchain event listener and one sync command per blockchain
    for blockchain_id in blockchain_ids {
        requests.push(CommandExecutionRequest::new(
            Command::BlockchainEventListener(BlockchainEventListenerCommandData::new(
                blockchain_id.clone(),
            )),
        ));
        requests.push(CommandExecutionRequest::new(Command::Sync(
            SyncCommandData::new(blockchain_id.clone()),
        )));
    }

    requests
}
