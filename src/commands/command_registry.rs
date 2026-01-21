use std::sync::Arc;

use blockchain::BlockchainId;

use super::{
    command_executor::CommandExecutionRequest,
    operations::{
        get::protocols::get::{
            handle_get_request_command::{
                HandleGetRequestCommandData, HandleGetRequestCommandHandler,
            },
            send_get_requests_command::{
                SendGetRequestsCommandData, SendGetRequestsCommandHandler,
            },
        },
        publish::protocols::{
            finality::{
                handle_finality_request_command::{
                    HandleFinalityRequestCommandData, HandleFinalityRequestCommandHandler,
                },
                send_finality_request_command::{
                    SendFinalityRequestCommandData, SendFinalityRequestCommandHandler,
                },
            },
            store::{
                handle_store_request_command::{
                    HandleStoreRequestCommandData, HandleStoreRequestCommandHandler,
                },
                send_store_requests_command::{
                    SendStoreRequestsCommandData, SendStoreRequestsCommandHandler,
                },
            },
        },
    },
    periodic::{
        blockchain_event_listener_command::{
            BlockchainEventListenerCommandData, BlockchainEventListenerCommandHandler,
        },
        dial_peers_command::{DialPeersCommandData, DialPeersCommandHandler},
        sharding_table_check_command::{
            ShardingTableCheckCommandData, ShardingTableCheckCommandHandler,
        },
    },
};
use crate::{commands::command_executor::CommandExecutionResult, context::Context};

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
    send_store_requests: SendStoreRequests => {
        data: SendStoreRequestsCommandData,
        handler: SendStoreRequestsCommandHandler
    },
    handle_store_request: HandleStoreRequest => {
        data: HandleStoreRequestCommandData,
        handler: HandleStoreRequestCommandHandler
    },
    send_finality_request: SendFinalityRequest => {
        data: SendFinalityRequestCommandData,
        handler: SendFinalityRequestCommandHandler
    },
    handle_finality_request: HandleFinalityRequest => {
        data: HandleFinalityRequestCommandData,
        handler: HandleFinalityRequestCommandHandler
    },
    send_get_requests: SendGetRequests => {
        data: SendGetRequestsCommandData,
        handler: SendGetRequestsCommandHandler
    },
    handle_get_request: HandleGetRequest => {
        data: HandleGetRequestCommandData,
        handler: HandleGetRequestCommandHandler
    }
}

/// Default commands scheduled at startup. Keep this list explicit for clarity.
pub fn default_command_requests(blockchain_ids: &[BlockchainId]) -> Vec<CommandExecutionRequest> {
    let mut requests = vec![
        CommandExecutionRequest::new(Command::DialPeers(DialPeersCommandData)),
        CommandExecutionRequest::new(Command::ShardingTableCheck(ShardingTableCheckCommandData)),
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
