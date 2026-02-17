use dkg_network::{
    ResponseMessage,
    message::{ResponseBody, ResponseMessageHeader, ResponseMessageType},
    messages::FinalityAck,
    request_response::ResponseChannel,
};
use uuid::Uuid;

use super::handler::HandlePublishFinalityRequestCommandHandler;

impl HandlePublishFinalityRequestCommandHandler {
    pub(crate) async fn send_response(
        &self,
        channel: ResponseChannel<ResponseMessage<FinalityAck>>,
        operation_id: Uuid,
        message: ResponseMessage<FinalityAck>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_finality_response(channel, message)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send finality response"
            );
        }
    }

    pub(crate) async fn send_ack(
        &self,
        channel: ResponseChannel<ResponseMessage<FinalityAck>>,
        operation_id: Uuid,
        ual: &str,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Ack),
            data: ResponseBody::ack(FinalityAck {
                message: format!("Acknowledged storing of {}", ual),
            }),
        };
        self.send_response(channel, operation_id, message).await;
    }

    pub(crate) async fn send_nack(
        &self,
        channel: ResponseChannel<ResponseMessage<FinalityAck>>,
        operation_id: Uuid,
        ual: &str,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Nack),
            data: ResponseBody::error(format!("Failed to acknowledge storing of {}", ual)),
        };
        self.send_response(channel, operation_id, message).await;
    }
}
