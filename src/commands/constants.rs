use super::command::CommandName;

pub const COMMAND_QUEUE_PARALLELISM: usize = 100;
pub const MAX_COMMAND_DELAY_MS: i64 = 10 * 24 * 60 * 60 * 1000;
pub const DEFAULT_COMMAND_DELAY_MS: i64 = 60 * 1000;
pub const DEFAULT_COMMAND_REPEAT_INTERVAL_MS: i64 = 5000;
pub const PERMANENT_COMMANDS: [CommandName; 1] = [CommandName::DialPeers];
