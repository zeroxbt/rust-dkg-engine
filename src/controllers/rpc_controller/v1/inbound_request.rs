use dkg_network::{PeerId, ResponseMessage, request_response::ResponseChannel};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::{
    commands::{executor::CommandExecutionRequest, scheduler::CommandScheduler},
    state::ResponseChannels,
};

/// Store the response channel and try to schedule command execution.
///
/// Returns `None` if the command was accepted. Returns `Some(channel)` if the
/// scheduler is full/closed so caller can send an immediate Busy response.
pub(super) fn store_channel_and_try_schedule<T>(
    response_channels: &ResponseChannels<T>,
    command_scheduler: &CommandScheduler,
    remote_peer_id: &PeerId,
    operation_id: Uuid,
    channel: ResponseChannel<ResponseMessage<T>>,
    command_request: CommandExecutionRequest,
) -> Option<ResponseChannel<ResponseMessage<T>>>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    response_channels.store(remote_peer_id, operation_id, channel);

    if !command_scheduler.try_schedule(command_request) {
        return response_channels.retrieve(remote_peer_id, operation_id);
    }

    None
}
