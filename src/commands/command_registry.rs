use std::sync::Arc;

use blockchain::BlockchainId;

use super::{
    command_executor::CommandExecutionRequest,
    periodic::{
        blockchain_event_listener_command::{
            BlockchainEventListenerCommandData, BlockchainEventListenerCommandHandler,
        },
        dial_peers_command::{DialPeersCommandData, DialPeersCommandHandler},
        sharding_table_check_command::{
            ShardingTableCheckCommandData, ShardingTableCheckCommandHandler,
        },
    },
    protocols::publish::{
        handle_publish_request_command::{
            HandlePublishRequestCommandData, HandlePublishRequestCommandHandler,
        },
        send_publish_requests_command::{
            SendPublishRequestsCommandData, SendPublishRequestsCommandHandler,
        },
    },
};
use crate::{
    commands::{
        command_executor::CommandExecutionResult,
        protocols::publish::finalize_publish_operation_command::{
            FinalizePublishOperationCommandData, FinalizePublishOperationCommandHandler,
        },
    },
    context::Context,
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
        pub enum Command {
            $( $variant($data), )+
        }

        impl Command {
            pub fn name(&self) -> &'static str {
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

        pub struct CommandResolver {
            $( $field: Arc<$handler>, )+
        }

        impl CommandResolver {
            pub fn new(context: Arc<Context>) -> Self {
                Self {
                    $( $field: Arc::new(<$handler>::new(Arc::clone(&context))), )+
                }
            }

            pub async fn execute(&self, command: &Command) -> CommandExecutionResult {
                match command {
                    $( Command::$variant(data) => self.$field.execute(data).await, )+
                }
            }

        }
    };
}

pub trait CommandHandler<D: Send + Sync + 'static>: Send + Sync {
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
    send_publish_requests: SendPublishRequests => {
        data: SendPublishRequestsCommandData,
        handler: SendPublishRequestsCommandHandler
    },
    handle_publish_request: HandlePublishRequest => {
        data: HandlePublishRequestCommandData,
        handler: HandlePublishRequestCommandHandler
    },
    finalize_publish_operation_command: FinalizePublishOperation => {
        data: FinalizePublishOperationCommandData,
        handler: FinalizePublishOperationCommandHandler
    }
}

/// Default commands scheduled at startup. Keep this list explicit for clarity.
pub fn default_command_requests(blockchain_ids: &[BlockchainId]) -> Vec<CommandExecutionRequest> {
    let mut requests = vec![
        CommandExecutionRequest::new(Command::DialPeers(DialPeersCommandData::default())),
        CommandExecutionRequest::new(Command::ShardingTableCheck(
            ShardingTableCheckCommandData::default(),
        )),
    ];

    // Schedule one blockchain event listener per blockchain
    for blockchain_id in blockchain_ids {
        requests.push(CommandExecutionRequest::new(
            Command::BlockchainEventListener(BlockchainEventListenerCommandData::new(
                blockchain_id.clone(),
            )),
        ));
    }

    requests
}
