use std::sync::Arc;

use uuid::Uuid;

use super::{PeerRateLimiter, RpcConfig};
use crate::{
    context::Context,
    controllers::rpc_controller::v1::{
        batch_get::BatchGetRpcController, get::GetRpcController,
        publish_finality::PublishFinalityRpcController, publish_store::PublishStoreRpcController,
    },
    managers::network::{
        NetworkEventHandler, NetworkManager, PeerId,
        message::{
            RequestMessage, ResponseBody, ResponseMessage, ResponseMessageHeader,
            ResponseMessageType,
        },
        messages::{
            BatchGetAck, BatchGetRequestData, FinalityAck, FinalityRequestData, GetAck,
            GetRequestData, StoreAck, StoreRequestData,
        },
        request_response::ResponseChannel,
    },
};

pub(crate) struct RpcRouter {
    network_manager: Arc<NetworkManager>,
    store_controller: Arc<PublishStoreRpcController>,
    get_controller: Arc<GetRpcController>,
    finality_controller: Arc<PublishFinalityRpcController>,
    batch_get_controller: Arc<BatchGetRpcController>,
    peer_rate_limiter: Arc<PeerRateLimiter>,
}

impl RpcRouter {
    pub(crate) fn new(context: Arc<Context>, config: &RpcConfig) -> Self {
        RpcRouter {
            network_manager: Arc::clone(context.network_manager()),
            store_controller: Arc::new(PublishStoreRpcController::new(Arc::clone(&context))),
            get_controller: Arc::new(GetRpcController::new(Arc::clone(&context))),
            finality_controller: Arc::new(PublishFinalityRpcController::new(Arc::clone(&context))),
            batch_get_controller: Arc::new(BatchGetRpcController::new(Arc::clone(&context))),
            peer_rate_limiter: Arc::new(PeerRateLimiter::new(config.rate_limiter.clone())),
        }
    }

    fn send_store_busy(
        &self,
        channel: ResponseChannel<ResponseMessage<StoreAck>>,
        operation_id: Uuid,
    ) {
        let response = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Busy),
            data: ResponseBody::error("Rate limited"),
        };
        if let Err(error) = self
            .network_manager
            .try_send_store_response(channel, response)
        {
            tracing::warn!(
                %operation_id,
                %error,
                "Failed to enqueue store busy response"
            );
        }
    }

    fn send_get_busy(
        &self,
        channel: ResponseChannel<ResponseMessage<GetAck>>,
        operation_id: Uuid,
    ) {
        let response = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Busy),
            data: ResponseBody::error("Rate limited"),
        };
        if let Err(error) = self
            .network_manager
            .try_send_get_response(channel, response)
        {
            tracing::warn!(
                %operation_id,
                %error,
                "Failed to enqueue get busy response"
            );
        }
    }

    fn send_finality_busy(
        &self,
        channel: ResponseChannel<ResponseMessage<FinalityAck>>,
        operation_id: Uuid,
    ) {
        let response = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Busy),
            data: ResponseBody::error("Rate limited"),
        };
        if let Err(error) = self
            .network_manager
            .try_send_finality_response(channel, response)
        {
            tracing::warn!(
                %operation_id,
                %error,
                "Failed to enqueue finality busy response"
            );
        }
    }

    fn send_batch_get_busy(
        &self,
        channel: ResponseChannel<ResponseMessage<BatchGetAck>>,
        operation_id: Uuid,
    ) {
        let response = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Busy),
            data: ResponseBody::error("Rate limited"),
        };
        if let Err(error) = self
            .network_manager
            .try_send_batch_get_response(channel, response)
        {
            tracing::warn!(
                %operation_id,
                %error,
                "Failed to enqueue batch-get busy response"
            );
        }
    }
}

impl NetworkEventHandler for RpcRouter {
    // ─────────────────────────────────────────────────────────────────────────
    // Protocol inbound requests
    // ─────────────────────────────────────────────────────────────────────────

    async fn on_store_request(
        &self,
        request: RequestMessage<StoreRequestData>,
        channel: ResponseChannel<ResponseMessage<StoreAck>>,
        peer: PeerId,
    ) {
        let operation_id = request.header.operation_id();
        if !self.peer_rate_limiter.check(&peer) {
            self.send_store_busy(channel, operation_id);
            return;
        }
        if let Some(channel) = self.store_controller.handle_request(request, channel, peer) {
            self.send_store_busy(channel, operation_id);
        }
    }

    async fn on_get_request(
        &self,
        request: RequestMessage<GetRequestData>,
        channel: ResponseChannel<ResponseMessage<GetAck>>,
        peer: PeerId,
    ) {
        let operation_id = request.header.operation_id();
        if !self.peer_rate_limiter.check(&peer) {
            self.send_get_busy(channel, operation_id);
            return;
        }
        if let Some(channel) = self.get_controller.handle_request(request, channel, peer) {
            self.send_get_busy(channel, operation_id);
        }
    }

    async fn on_finality_request(
        &self,
        request: RequestMessage<FinalityRequestData>,
        channel: ResponseChannel<ResponseMessage<FinalityAck>>,
        peer: PeerId,
    ) {
        let operation_id = request.header.operation_id();
        if !self.peer_rate_limiter.check(&peer) {
            self.send_finality_busy(channel, operation_id);
            return;
        }
        if let Some(channel) = self
            .finality_controller
            .handle_request(request, channel, peer)
        {
            self.send_finality_busy(channel, operation_id);
        }
    }

    async fn on_batch_get_request(
        &self,
        request: RequestMessage<BatchGetRequestData>,
        channel: ResponseChannel<ResponseMessage<BatchGetAck>>,
        peer: PeerId,
    ) {
        let operation_id = request.header.operation_id();
        if !self.peer_rate_limiter.check(&peer) {
            self.send_batch_get_busy(channel, operation_id);
            return;
        }
        if let Some(channel) = self
            .batch_get_controller
            .handle_request(request, channel, peer)
        {
            self.send_batch_get_busy(channel, operation_id);
        }
    }
}
