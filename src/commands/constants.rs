use std::time::Duration;

/// Maximum number of commands that can execute concurrently.
/// Set to ~2x CPU cores for I/O-bound command workloads.
pub(crate) const COMMAND_CONCURRENT_LIMIT: usize = 32;

/// Reserved permits for periodic commands.
/// Defaults to ~25% of total, rounding up.
pub(crate) const PERIODIC_COMMAND_CONCURRENT_LIMIT: usize = COMMAND_CONCURRENT_LIMIT.div_ceil(4);

/// Permits for non-periodic commands.
pub(crate) const GENERAL_COMMAND_CONCURRENT_LIMIT: usize =
    COMMAND_CONCURRENT_LIMIT - PERIODIC_COMMAND_CONCURRENT_LIMIT;

/// Buffer size for the command queue channel.
/// Allows absorbing bursts without immediately blocking callers.
pub(crate) const COMMAND_QUEUE_SIZE: usize = 256;

/// Maximum lifetime for a command before it expires (5 minutes)
pub(crate) const MAX_COMMAND_LIFETIME: Duration = Duration::from_mins(5);
