use dkg_network::{
    BatchGetAck, BatchGetRequestData, FinalityAck, FinalityRequestData, GetAck, GetRequestData,
    ImmediateResponse, InboundDecision, InboundRequest, NetworkEventHandler,
    PROTOCOL_NAME_BATCH_GET, PROTOCOL_NAME_FINALITY, PROTOCOL_NAME_GET, PROTOCOL_NAME_STORE,
    ResponseHandle, StoreAck, StoreRequestData,
};
use dkg_observability as observability;

use super::{PeerRateLimiter, RpcConfig, deps::RpcRouterDeps};
use crate::commands::{
    executor::CommandExecutionRequest,
    operations::{
        batch_get::handle_batch_get_request::HandleBatchGetRequestCommandData,
        get::handle_get_request::HandleGetRequestCommandData,
        publish::{
            finality::handle_publish_finality_request::HandlePublishFinalityRequestCommandData,
            store::handle_publish_store_request::HandlePublishStoreRequestCommandData,
        },
    },
    registry::Command,
    scheduler::CommandScheduler,
};

pub(crate) struct RpcRouter {
    command_scheduler: CommandScheduler,
    peer_rate_limiter: PeerRateLimiter,
}

impl RpcRouter {
    pub(crate) fn new(deps: RpcRouterDeps, config: &RpcConfig) -> Self {
        RpcRouter {
            command_scheduler: deps.command_scheduler,
            peer_rate_limiter: PeerRateLimiter::new(config.rate_limiter.clone()),
        }
    }

    fn busy_response<T>(
        response_handle: ResponseHandle<T>,
        operation_id: uuid::Uuid,
        error_message: &'static str,
    ) -> ImmediateResponse<T> {
        ImmediateResponse::busy(response_handle, operation_id, error_message)
    }

    fn route_inbound_request<TReq, TAck>(
        &self,
        protocol: &'static str,
        request: InboundRequest<TReq>,
        response_handle: ResponseHandle<TAck>,
        schedule: impl FnOnce(
            &Self,
            InboundRequest<TReq>,
            ResponseHandle<TAck>,
        ) -> Option<ResponseHandle<TAck>>,
    ) -> InboundDecision<TAck> {
        let operation_id = request.operation_id();

        if !self.peer_rate_limiter.check(request.peer_id()) {
            observability::record_network_inbound_request(protocol, "rate_limited");
            return InboundDecision::RespondNow(Self::busy_response(
                response_handle,
                operation_id,
                "Rate limited",
            ));
        }

        if let Some(response_handle) = schedule(self, request, response_handle) {
            observability::record_network_inbound_request(protocol, "scheduler_rejected");
            return InboundDecision::RespondNow(Self::busy_response(
                response_handle,
                operation_id,
                "Busy",
            ));
        }

        observability::record_network_inbound_request(protocol, "scheduled");
        InboundDecision::Scheduled
    }

    fn schedule_store_request(
        &self,
        request: InboundRequest<StoreRequestData>,
        response_handle: ResponseHandle<StoreAck>,
    ) -> Option<ResponseHandle<StoreAck>> {
        tracing::trace!(
            operation_id = %request.operation_id(),
            dataset_root = %request.data().dataset_root(),
            peer = %request.peer_id(),
            "Store request received"
        );

        let command = Command::HandlePublishStoreRequest(
            HandlePublishStoreRequestCommandData::new(request, response_handle),
        );
        match self
            .command_scheduler
            .try_schedule(CommandExecutionRequest::new(command))
        {
            Ok(()) => None,
            Err(request) => match request.into_command() {
                Command::HandlePublishStoreRequest(command) => Some(command.response_handle),
                _ => unreachable!("unexpected rejected command type"),
            },
        }
    }

    fn schedule_get_request(
        &self,
        request: InboundRequest<GetRequestData>,
        response_handle: ResponseHandle<GetAck>,
    ) -> Option<ResponseHandle<GetAck>> {
        tracing::trace!(
            operation_id = %request.operation_id(),
            ual = %request.data().ual(),
            peer = %request.peer_id(),
            "Get request received"
        );

        let command =
            Command::HandleGetRequest(HandleGetRequestCommandData::new(request, response_handle));
        match self
            .command_scheduler
            .try_schedule(CommandExecutionRequest::new(command))
        {
            Ok(()) => None,
            Err(request) => match request.into_command() {
                Command::HandleGetRequest(command) => Some(command.response_handle),
                _ => unreachable!("unexpected rejected command type"),
            },
        }
    }

    fn schedule_finality_request(
        &self,
        request: InboundRequest<FinalityRequestData>,
        response_handle: ResponseHandle<FinalityAck>,
    ) -> Option<ResponseHandle<FinalityAck>> {
        tracing::trace!(
            operation_id = %request.operation_id(),
            publish_operation_id = %request.data().publish_operation_id(),
            ual = %request.data().ual(),
            peer = %request.peer_id(),
            "Finality request received"
        );

        let command = Command::HandlePublishFinalityRequest(
            HandlePublishFinalityRequestCommandData::new(request, response_handle),
        );
        match self
            .command_scheduler
            .try_schedule(CommandExecutionRequest::new(command))
        {
            Ok(()) => None,
            Err(request) => match request.into_command() {
                Command::HandlePublishFinalityRequest(command) => Some(command.response_handle),
                _ => unreachable!("unexpected rejected command type"),
            },
        }
    }

    fn schedule_batch_get_request(
        &self,
        request: InboundRequest<BatchGetRequestData>,
        response_handle: ResponseHandle<BatchGetAck>,
    ) -> Option<ResponseHandle<BatchGetAck>> {
        tracing::trace!(
            operation_id = %request.operation_id(),
            ual_count = request.data().uals().len(),
            peer = %request.peer_id(),
            "Batch get request received"
        );

        let command = Command::HandleBatchGetRequest(HandleBatchGetRequestCommandData::new(
            request,
            response_handle,
        ));
        match self
            .command_scheduler
            .try_schedule(CommandExecutionRequest::new(command))
        {
            Ok(()) => None,
            Err(request) => match request.into_command() {
                Command::HandleBatchGetRequest(command) => Some(command.response_handle),
                _ => unreachable!("unexpected rejected command type"),
            },
        }
    }
}

impl NetworkEventHandler for RpcRouter {
    // ─────────────────────────────────────────────────────────────────────────
    // Protocol inbound requests
    // ─────────────────────────────────────────────────────────────────────────

    fn on_store_request(
        &self,
        request: InboundRequest<StoreRequestData>,
        response_handle: ResponseHandle<StoreAck>,
    ) -> InboundDecision<StoreAck> {
        self.route_inbound_request(
            PROTOCOL_NAME_STORE,
            request,
            response_handle,
            Self::schedule_store_request,
        )
    }

    fn on_get_request(
        &self,
        request: InboundRequest<GetRequestData>,
        response_handle: ResponseHandle<GetAck>,
    ) -> InboundDecision<GetAck> {
        self.route_inbound_request(
            PROTOCOL_NAME_GET,
            request,
            response_handle,
            Self::schedule_get_request,
        )
    }

    fn on_finality_request(
        &self,
        request: InboundRequest<FinalityRequestData>,
        response_handle: ResponseHandle<FinalityAck>,
    ) -> InboundDecision<FinalityAck> {
        self.route_inbound_request(
            PROTOCOL_NAME_FINALITY,
            request,
            response_handle,
            Self::schedule_finality_request,
        )
    }

    fn on_batch_get_request(
        &self,
        request: InboundRequest<BatchGetRequestData>,
        response_handle: ResponseHandle<BatchGetAck>,
    ) -> InboundDecision<BatchGetAck> {
        self.route_inbound_request(
            PROTOCOL_NAME_BATCH_GET,
            request,
            response_handle,
            Self::schedule_batch_get_request,
        )
    }
}
