use std::time::Duration;

pub(crate) const COMMAND_QUEUE_PARALLELISM: usize = 100;

/// Maximum delay for a command before execution (24 hours)
pub(crate) const MAX_COMMAND_DELAY: Duration = Duration::from_hours(24);

/// Maximum lifetime for a command before it expires (5 minutes)
pub(crate) const MAX_COMMAND_LIFETIME: Duration = Duration::from_mins(5);
