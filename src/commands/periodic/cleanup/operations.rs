use std::time::Duration;

use chrono::Utc;
use uuid::Uuid;

use crate::{
    managers::repository::{OperationStatus, RepositoryManager},
    services::OperationStatusService,
};

pub(crate) async fn cleanup_operations(
    repository: &RepositoryManager,
    publish_store_results: &OperationStatusService<crate::operations::PublishStoreOperationResult>,
    get_results: &OperationStatusService<crate::operations::GetOperationResult>,
    ttl: Duration,
    batch_size: usize,
) -> Result<usize, String> {
    if batch_size == 0 {
        return Ok(0);
    }

    let cutoff = Utc::now() - chrono::Duration::seconds(i64::try_from(ttl.as_secs()).unwrap_or(0));
    let statuses = [OperationStatus::Completed, OperationStatus::Failed];

    let mut total_removed = 0usize;

    loop {
        let ids = repository
            .operation_repository()
            .find_ids_by_status_older_than(&statuses, cutoff, batch_size as u64)
            .await
            .map_err(|e| e.to_string())?;

        if ids.is_empty() {
            break;
        }

        remove_results(publish_store_results, get_results, &ids);

        let removed_rows = repository
            .operation_repository()
            .delete_by_ids(&ids)
            .await
            .map_err(|e| e.to_string())?;

        total_removed = total_removed.saturating_add(removed_rows as usize);

        if ids.len() < batch_size {
            break;
        }
    }

    Ok(total_removed)
}

fn remove_results(
    publish_store_results: &OperationStatusService<crate::operations::PublishStoreOperationResult>,
    get_results: &OperationStatusService<crate::operations::GetOperationResult>,
    ids: &[Uuid],
) {
    for id in ids {
        if let Err(e) = publish_store_results.remove_result(*id) {
            tracing::warn!(
                operation_id = %id,
                error = %e,
                "Failed to remove cached publish operation result"
            );
        }
        if let Err(e) = get_results.remove_result(*id) {
            tracing::warn!(
                operation_id = %id,
                error = %e,
                "Failed to remove cached get operation result"
            );
        }
    }
}
