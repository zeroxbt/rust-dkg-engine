use std::time::Duration;

use metrics::{counter, histogram};

pub(crate) fn record_repository_query(
    repository: &str,
    method: &str,
    status: &str,
    duration: Duration,
    rows: Option<usize>,
) {
    counter!(
        "node_repository_query_total",
        "repository" => repository.to_string(),
        "method" => method.to_string(),
        "status" => status.to_string()
    )
    .increment(1);

    histogram!(
        "node_repository_query_duration_seconds",
        "repository" => repository.to_string(),
        "method" => method.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());

    if let Some(rows) = rows {
        histogram!(
            "node_repository_query_rows",
            "repository" => repository.to_string(),
            "method" => method.to_string(),
            "status" => status.to_string()
        )
        .record(rows as f64);
    }
}
