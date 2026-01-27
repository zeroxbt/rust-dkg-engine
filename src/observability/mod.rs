//! Observability utilities for metrics and instrumentation.
//!
//! This module provides helper functions to instrument async operations
//! with both timing metrics and success/failure counters.

use std::{future::Future, time::Instant};

use metrics::{counter, histogram};

/// Records duration and success/failure for an async operation that returns `Result`.
///
/// This function wraps an async operation and automatically records:
/// - A histogram of operation duration in seconds
/// - A counter of operations with success/error status
///
/// # Example
///
/// ```ignore
/// use crate::observability::observe;
///
/// async fn insert_data(&self, data: &Data) -> Result<(), Error> {
///     observe("triple_store", "insert", self.inner.insert(data)).await
/// }
/// ```
///
/// This will record:
/// - `triple_store_operation_duration_seconds{operation="insert"}` histogram
/// - `triple_store_operations_total{operation="insert", status="success|error"}` counter
pub(crate) async fn observe<T, E, F>(
    subsystem: &'static str,
    operation: &'static str,
    fut: F,
) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let start = Instant::now();
    let result = fut.await;
    let duration = start.elapsed();
    let status = if result.is_ok() { "success" } else { "error" };

    histogram!(
        format!("{subsystem}_operation_duration_seconds"),
        "operation" => operation,
    )
    .record(duration.as_secs_f64());

    counter!(
        format!("{subsystem}_operations_total"),
        "operation" => operation,
        "status" => status,
    )
    .increment(1);

    result
}

/// Records duration for an infallible async operation.
///
/// Similar to [`observe`], but for operations that don't return a `Result`.
/// Always records status as "success".
///
/// # Example
///
/// ```ignore
/// use crate::observability::observe_infallible;
///
/// async fn get_peers(&self) -> Vec<PeerId> {
///     observe_infallible("network", "get_peers", self.inner.get_peers()).await
/// }
/// ```
pub(crate) async fn observe_infallible<T, F>(
    subsystem: &'static str,
    operation: &'static str,
    fut: F,
) -> T
where
    F: Future<Output = T>,
{
    let start = Instant::now();
    let result = fut.await;
    let duration = start.elapsed();

    histogram!(
        format!("{subsystem}_operation_duration_seconds"),
        "operation" => operation,
    )
    .record(duration.as_secs_f64());

    counter!(
        format!("{subsystem}_operations_total"),
        "operation" => operation,
        "status" => "success",
    )
    .increment(1);

    result
}
