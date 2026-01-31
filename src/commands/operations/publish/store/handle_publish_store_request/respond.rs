use uuid::Uuid;

use super::handler::HandlePublishStoreRequestCommandHandler;
use crate::managers::network::{
    ResponseMessage,
    message::{ResponseBody, ResponseMessageHeader, ResponseMessageType},
    messages::StoreAck,
    request_response::ResponseChannel,
};

impl HandlePublishStoreRequestCommandHandler {
    pub(crate) async fn send_response(
        &self,
        channel: ResponseChannel<ResponseMessage<StoreAck>>,
        operation_id: Uuid,
        message: ResponseMessage<StoreAck>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_store_response(channel, message)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send response"
            );
        }
    }

    pub(crate) async fn send_nack(
        &self,
        channel: ResponseChannel<ResponseMessage<StoreAck>>,
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
        channel: ResponseChannel<ResponseMessage<StoreAck>>,
        operation_id: Uuid,
        identity_id: u128,
        signature: crate::types::SignatureComponents,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Ack),
            data: ResponseBody::ack(StoreAck {
                identity_id,
                signature,
            }),
        };
        self.send_response(channel, operation_id, message).await;
    }
}
