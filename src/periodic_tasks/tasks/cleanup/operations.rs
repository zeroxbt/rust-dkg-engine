use std::time::Duration;

use chrono::Utc;
use dkg_repository::{OperationRepository, OperationStatus};
use uuid::Uuid;

use crate::{
    operations::{GetOperation, PublishStoreOperation},
    services::OperationStatusService,
};

pub(crate) async fn cleanup_operations(
    operation_repository: &OperationRepository,
    publish_store_results: &OperationStatusService<PublishStoreOperation>,
    get_results: &OperationStatusService<GetOperation>,
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
        let ids = operation_repository
            .find_ids_by_status_older_than(&statuses, cutoff, batch_size as u64)
            .await
            .map_err(|e| e.to_string())?;

        if ids.is_empty() {
            break;
        }

        remove_results(publish_store_results, get_results, &ids).await;

        let removed_rows = operation_repository
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

async fn remove_results(
    publish_store_results: &OperationStatusService<PublishStoreOperation>,
    get_results: &OperationStatusService<GetOperation>,
    ids: &[Uuid],
) {
    for id in ids {
        if let Err(e) = publish_store_results.remove_result(*id).await {
            tracing::warn!(
                operation_id = %id,
                error = %e,
                "Failed to remove cached publish operation result"
            );
        }
        if let Err(e) = get_results.remove_result(*id).await {
            tracing::warn!(
                operation_id = %id,
                error = %e,
                "Failed to remove cached get operation result"
            );
        }
    }
}
