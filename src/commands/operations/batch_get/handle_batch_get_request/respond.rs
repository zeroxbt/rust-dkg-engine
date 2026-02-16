use std::collections::HashMap;

use dkg_domain::Assertion;
use uuid::Uuid;

use crate::{
    commands::operations::batch_get::handle_batch_get_request::HandleBatchGetRequestCommandHandler,
    managers::network::{
        ResponseMessage,
        message::{ResponseBody, ResponseMessageHeader, ResponseMessageType},
        messages::BatchGetAck,
        request_response::ResponseChannel,
    },
};

impl HandleBatchGetRequestCommandHandler {
    async fn send_response(
        &self,
        channel: ResponseChannel<ResponseMessage<BatchGetAck>>,
        operation_id: Uuid,
        message: ResponseMessage<BatchGetAck>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_batch_get_response(channel, message)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send batch get response"
            );
        }
    }

    pub(crate) async fn send_ack(
        &self,
        channel: ResponseChannel<ResponseMessage<BatchGetAck>>,
        operation_id: Uuid,
        assertions: HashMap<String, Assertion>,
        metadata: HashMap<String, Vec<String>>,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Ack),
            data: ResponseBody::ack(BatchGetAck {
                assertions,
                metadata,
            }),
        };
        self.send_response(channel, operation_id, message).await;
    }

    pub(crate) async fn send_nack(
        &self,
        channel: ResponseChannel<ResponseMessage<BatchGetAck>>,
        operation_id: Uuid,
        message: impl Into<String>,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Nack),
            data: ResponseBody::error(message),
        };
        self.send_response(channel, operation_id, message).await;
    }
}
