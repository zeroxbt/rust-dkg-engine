use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use key_value_store::{KeyValueStoreManager, Table};
use libp2p::PeerId;
use network::{
    NetworkManager, RequestMessage,
    message::{RequestMessageHeader, RequestMessageType},
};
use repository::{OperationStatus, RepositoryManager};
use tokio::sync::watch;
use uuid::Uuid;

use super::{
    context_store::ContextStore,
    result_store::{ResultStoreError, TABLE_NAME},
    traits::Operation,
};
use crate::{
    controllers::rpc_controller::NetworkProtocols,
    error::NodeError,
    services::{PendingRequests, RequestError},
};

/// Generic operation service that handles all shared operation logic.
///
/// This service is parameterized by an `Operation` type that defines
/// the specific request/response/state/result types for each operation.
///
/// # Ownership
/// - `ContextStore<Op::State>` - owned here
/// - `Table<Op::Result>` - typed table for this operation's results
/// - Completion signals - owned here
/// - `PendingRequests<Op::Response>` - tracks outbound requests awaiting responses
/// - `NetworkManager` - shared reference for sending requests
pub struct OperationService<Op: Operation> {
    repository: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    context_store: ContextStore<Op::State>,
    result_table: Table<Op::Result>,
    completion_signals: DashMap<Uuid, watch::Sender<OperationStatus>>,
    pending_requests: PendingRequests<Op::Response>,
}

impl<Op: Operation> OperationService<Op> {
    /// Create a new operation service.
    pub fn new(
        repository: Arc<RepositoryManager>,
        network_manager: Arc<NetworkManager<NetworkProtocols>>,
        kv_store_manager: &KeyValueStoreManager,
    ) -> Result<Self, ResultStoreError> {
        let result_table = kv_store_manager.table(TABLE_NAME)?;
        Ok(Self {
            repository,
            network_manager,
            context_store: ContextStore::with_default_ttl(),
            result_table,
            completion_signals: DashMap::new(),
            pending_requests: PendingRequests::new(),
        })
    }

    /// Access the pending requests tracker.
    ///
    /// Used by RpcRouter to complete pending requests when responses arrive.
    pub fn pending_requests(&self) -> &PendingRequests<Op::Response> {
        &self.pending_requests
    }

    /// Send a request to a peer and await the response.
    ///
    /// This method provides a synchronous request-response pattern on top of
    /// libp2p's event-driven model. It:
    /// 1. Sends the request via the network manager
    /// 2. Registers a oneshot channel to receive the response
    /// 3. Awaits the response (or timeout/failure from libp2p)
    ///
    /// The response is delivered when RpcRouter receives the libp2p event and
    /// calls `pending_requests().complete_success()` or `complete_failure()`.
    ///
    /// # Arguments
    /// * `operation_id` - The operation identifier for logging/tracking
    /// * `peer` - The peer to send the request to
    /// * `request_data` - The request payload
    ///
    /// # Returns
    /// * `Ok(response)` - The peer's response
    /// * `Err(RequestError)` - Timeout, connection failure, or channel error
    pub async fn send_request(
        &self,
        operation_id: Uuid,
        peer: PeerId,
        request_data: Op::Request,
    ) -> Result<Op::Response, RequestError> {
        let message = RequestMessage {
            header: RequestMessageHeader::new(operation_id, RequestMessageType::ProtocolRequest),
            data: request_data,
        };

        let protocol_request = Op::build_protocol_request(peer, message);

        // Send the request and get the request ID
        let request_id = self
            .network_manager
            .send_protocol_request(protocol_request)
            .await
            .map_err(|e| RequestError::ConnectionFailed(e.to_string()))?;

        // Register the pending request and get a receiver for the response
        let response_rx = self.pending_requests.insert(request_id);

        tracing::debug!(
            operation_id = %operation_id,
            peer = %peer,
            ?request_id,
            "[{}] Request sent, awaiting response",
            Op::NAME
        );

        // Await the response
        response_rx
            .await
            .map_err(|_| RequestError::ChannelClosed)?
    }

    /// Create a new operation record in the database and return a completion receiver.
    ///
    /// The returned watch receiver can be used by external callers to await operation
    /// completion. The receiver will be updated when `mark_completed` or `mark_failed`
    /// is called.
    ///
    /// For callers that just want to trigger an operation without awaiting, the receiver
    /// can be dropped immediately.
    pub async fn create_operation(
        &self,
        operation_id: Uuid,
    ) -> Result<watch::Receiver<OperationStatus>, NodeError> {
        self.repository
            .operation_repository()
            .create(
                operation_id,
                Op::NAME,
                OperationStatus::InProgress.as_str(),
                Utc::now().timestamp_millis(),
            )
            .await?;

        // Create completion signal for external observers
        let (tx, rx) = watch::channel(OperationStatus::InProgress);
        self.completion_signals.insert(operation_id, tx);

        tracing::debug!(
            operation_id = %operation_id,
            "[{}] Operation record created with completion signal",
            Op::NAME
        );

        Ok(rx)
    }

    /// Store operation context/state for response processing.
    pub fn store_context(&self, operation_id: Uuid, state: Op::State) {
        self.context_store.store(operation_id, state);
    }

    /// Get operation context (does not remove it).
    pub fn get_context(&self, operation_id: &Uuid) -> Option<Op::State> {
        self.context_store.get(operation_id)
    }

    /// Remove operation context.
    pub fn remove_context(&self, operation_id: &Uuid) -> Option<Op::State> {
        self.context_store.remove(operation_id)
    }

    /// Store a result in the key-value store.
    pub fn store_result(
        &self,
        operation_id: Uuid,
        result: &Op::Result,
    ) -> Result<(), ResultStoreError> {
        self.result_table.store(operation_id, result)?;
        tracing::debug!(
            operation_id = %operation_id,
            "[{}] Result stored",
            Op::NAME
        );
        Ok(())
    }

    /// Get a cached operation result from the key-value store.
    pub fn get_result(&self, operation_id: Uuid) -> Result<Option<Op::Result>, ResultStoreError> {
        Ok(self.result_table.get(operation_id)?)
    }

    /// Update a result in the key-value store using a closure.
    /// If no result exists, creates one from the default value.
    /// This enables incremental updates (e.g., adding signatures one at a time).
    pub fn update_result<F>(
        &self,
        operation_id: Uuid,
        default: Op::Result,
        update_fn: F,
    ) -> Result<(), ResultStoreError>
    where
        F: FnOnce(&mut Op::Result),
    {
        self.result_table.update(operation_id, default, update_fn)?;
        tracing::trace!(
            operation_id = %operation_id,
            "[{}] Result updated",
            Op::NAME
        );
        Ok(())
    }

    /// Manually complete an operation with the final response counts.
    /// Caller should store the result first via `store_result` before calling this.
    ///
    /// # Arguments
    /// * `operation_id` - The operation identifier
    /// * `success_count` - Number of successful responses received
    /// * `failure_count` - Number of failed responses received
    pub async fn mark_completed(
        &self,
        operation_id: Uuid,
        success_count: u16,
        failure_count: u16,
    ) -> Result<(), NodeError> {
        // Update progress counts first
        self.repository
            .operation_repository()
            .update_progress(operation_id, success_count, failure_count)
            .await?;

        // Update status in database
        self.repository
            .operation_repository()
            .update_status(operation_id, OperationStatus::Completed.as_str())
            .await?;

        // Signal completion
        self.signal_completion(operation_id, OperationStatus::Completed);

        tracing::info!(
            operation_id = %operation_id,
            success_count = success_count,
            failure_count = failure_count,
            "[{}] Operation marked as completed",
            Op::NAME
        );

        Ok(())
    }

    /// Manually fail an operation (e.g., due to external error before sending requests).
    pub async fn mark_failed(&self, operation_id: Uuid, reason: String) {
        let result = self
            .repository
            .operation_repository()
            .update(
                operation_id,
                Some(OperationStatus::Failed.as_str()),
                Some(reason),
                None,
            )
            .await;

        // Signal failure
        self.signal_completion(operation_id, OperationStatus::Failed);

        // Clean up
        let _ = self.result_table.remove(operation_id);
        self.remove_context(&operation_id);

        match result {
            Ok(_) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    "[{}] Operation failed",
                    Op::NAME
                );
            }
            Err(e) => {
                tracing::error!(
                    operation_id = %operation_id,
                    error = %e,
                    "Unable to mark operation as failed"
                );
            }
        }
    }

    /// Clean up resources after operation completion.
    pub fn cleanup(&self, operation_id: &Uuid) {
        self.completion_signals.remove(operation_id);
        self.remove_context(operation_id);
    }

    /// Signal completion status to external observers.
    fn signal_completion(&self, operation_id: Uuid, status: OperationStatus) {
        if let Some(entry) = self.completion_signals.get(&operation_id) {
            let _ = entry.send(status);
            tracing::debug!(
                operation_id = %operation_id,
                status = ?status,
                "[{}] Signaled completion",
                Op::NAME
            );
        }
    }
}
