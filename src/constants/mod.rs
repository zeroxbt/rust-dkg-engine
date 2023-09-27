use crate::commands::command::CommandName;

pub const DIAL_PEERS_COMMAND_FREQUENCY_MILLS: i64 = 30_000;

pub const MAX_COMMAND_DELAY_IN_MILLS: i64 = 14400 * 60 * 1000;

pub const DEFAULT_COMMAND_REPEAT_INTERVAL_IN_MILLS: i64 = 5000; // 5 seconds

pub const COMMAND_QUEUE_PARALLELISM: usize = 100;

pub const PERMANENT_COMMANDS: [CommandName; 1] = [CommandName::DialPeers];
