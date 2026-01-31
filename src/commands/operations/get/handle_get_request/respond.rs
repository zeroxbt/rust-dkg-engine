use uuid::Uuid;

use crate::{
    commands::operations::get::handle_get_request::HandleGetRequestCommandHandler,
    managers::{
        network::{
            ResponseMessage,
            message::{ResponseBody, ResponseMessageHeader, ResponseMessageType},
            messages::GetAck,
            request_response::ResponseChannel,
        },
        triple_store::Assertion,
    },
};

impl HandleGetRequestCommandHandler {
    async fn send_response(
        &self,
        channel: ResponseChannel<ResponseMessage<GetAck>>,
        operation_id: Uuid,
        message: ResponseMessage<GetAck>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_get_response(channel, message)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send get response"
            );
        }
    }

    pub(crate) async fn send_nack(
        &self,
        channel: ResponseChannel<ResponseMessage<GetAck>>,
        operation_id: Uuid,
        error_message: &str,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Nack),
            data: ResponseBody::error(error_message),
        };
        self.send_response(channel, operation_id, message).await;
    }

    pub(crate) async fn send_ack(
        &self,
        channel: ResponseChannel<ResponseMessage<GetAck>>,
        operation_id: Uuid,
        assertion: Assertion,
        metadata: Option<Vec<String>>,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Ack),
            data: ResponseBody::ack(GetAck {
                assertion,
                metadata,
            }),
        };
        self.send_response(channel, operation_id, message).await;
    }
}
