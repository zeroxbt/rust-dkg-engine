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
        ImmediateResponse, InboundDecision, NetworkEventHandler, PeerId,
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
    store_controller: Arc<PublishStoreRpcController>,
    get_controller: Arc<GetRpcController>,
    finality_controller: Arc<PublishFinalityRpcController>,
    batch_get_controller: Arc<BatchGetRpcController>,
    peer_rate_limiter: Arc<PeerRateLimiter>,
}

impl RpcRouter {
    pub(crate) fn new(context: Arc<Context>, config: &RpcConfig) -> Self {
        RpcRouter {
            store_controller: Arc::new(PublishStoreRpcController::new(Arc::clone(&context))),
            get_controller: Arc::new(GetRpcController::new(Arc::clone(&context))),
            finality_controller: Arc::new(PublishFinalityRpcController::new(Arc::clone(&context))),
            batch_get_controller: Arc::new(BatchGetRpcController::new(Arc::clone(&context))),
            peer_rate_limiter: Arc::new(PeerRateLimiter::new(config.rate_limiter.clone())),
        }
    }

    fn store_busy_response(
        channel: ResponseChannel<ResponseMessage<StoreAck>>,
        operation_id: Uuid,
    ) -> ImmediateResponse<StoreAck> {
        let message = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Busy),
            data: ResponseBody::error("Rate limited"),
        };
        ImmediateResponse { channel, message }
    }

    fn get_busy_response(
        channel: ResponseChannel<ResponseMessage<GetAck>>,
        operation_id: Uuid,
    ) -> ImmediateResponse<GetAck> {
        let message = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Busy),
            data: ResponseBody::error("Rate limited"),
        };
        ImmediateResponse { channel, message }
    }

    fn finality_busy_response(
        channel: ResponseChannel<ResponseMessage<FinalityAck>>,
        operation_id: Uuid,
    ) -> ImmediateResponse<FinalityAck> {
        let message = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Busy),
            data: ResponseBody::error("Rate limited"),
        };
        ImmediateResponse { channel, message }
    }

    fn batch_get_busy_response(
        channel: ResponseChannel<ResponseMessage<BatchGetAck>>,
        operation_id: Uuid,
    ) -> ImmediateResponse<BatchGetAck> {
        let message = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Busy),
            data: ResponseBody::error("Rate limited"),
        };
        ImmediateResponse { channel, message }
    }
}

impl NetworkEventHandler for RpcRouter {
    // ─────────────────────────────────────────────────────────────────────────
    // Protocol inbound requests
    // ─────────────────────────────────────────────────────────────────────────

    fn on_store_request(
        &self,
        request: RequestMessage<StoreRequestData>,
        channel: ResponseChannel<ResponseMessage<StoreAck>>,
        peer: PeerId,
    ) -> InboundDecision<StoreAck> {
        let operation_id = request.header.operation_id();
        if !self.peer_rate_limiter.check(&peer) {
            return InboundDecision::RespondNow(Self::store_busy_response(channel, operation_id));
        }
        if let Some(channel) = self.store_controller.handle_request(request, channel, peer) {
            return InboundDecision::RespondNow(Self::store_busy_response(channel, operation_id));
        }
        InboundDecision::Scheduled
    }

    fn on_get_request(
        &self,
        request: RequestMessage<GetRequestData>,
        channel: ResponseChannel<ResponseMessage<GetAck>>,
        peer: PeerId,
    ) -> InboundDecision<GetAck> {
        let operation_id = request.header.operation_id();
        if !self.peer_rate_limiter.check(&peer) {
            return InboundDecision::RespondNow(Self::get_busy_response(channel, operation_id));
        }
        if let Some(channel) = self.get_controller.handle_request(request, channel, peer) {
            return InboundDecision::RespondNow(Self::get_busy_response(channel, operation_id));
        }
        InboundDecision::Scheduled
    }

    fn on_finality_request(
        &self,
        request: RequestMessage<FinalityRequestData>,
        channel: ResponseChannel<ResponseMessage<FinalityAck>>,
        peer: PeerId,
    ) -> InboundDecision<FinalityAck> {
        let operation_id = request.header.operation_id();
        if !self.peer_rate_limiter.check(&peer) {
            return InboundDecision::RespondNow(Self::finality_busy_response(
                channel,
                operation_id,
            ));
        }
        if let Some(channel) = self
            .finality_controller
            .handle_request(request, channel, peer)
        {
            return InboundDecision::RespondNow(Self::finality_busy_response(
                channel,
                operation_id,
            ));
        }
        InboundDecision::Scheduled
    }

    fn on_batch_get_request(
        &self,
        request: RequestMessage<BatchGetRequestData>,
        channel: ResponseChannel<ResponseMessage<BatchGetAck>>,
        peer: PeerId,
    ) -> InboundDecision<BatchGetAck> {
        let operation_id = request.header.operation_id();
        if !self.peer_rate_limiter.check(&peer) {
            return InboundDecision::RespondNow(Self::batch_get_busy_response(
                channel,
                operation_id,
            ));
        }
        if let Some(channel) = self
            .batch_get_controller
            .handle_request(request, channel, peer)
        {
            return InboundDecision::RespondNow(Self::batch_get_busy_response(
                channel,
                operation_id,
            ));
        }
        InboundDecision::Scheduled
    }
}
