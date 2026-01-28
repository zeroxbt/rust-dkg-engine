mod handle_batch_get_request_command;
mod send_batch_get_requests_command;

/// Maximum number of UALs allowed in a single batch get request.
pub(crate) const BATCH_GET_UAL_MAX_LIMIT: usize = 1000;

pub(crate) use handle_batch_get_request_command::{
    HandleBatchGetRequestCommandData, HandleBatchGetRequestCommandHandler,
};
