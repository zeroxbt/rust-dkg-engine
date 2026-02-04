use std::time::Duration;

use chrono::Utc;

use crate::managers::repository::RepositoryManager;

pub(crate) async fn cleanup_finality_acks(
    repository: &RepositoryManager,
    ttl: Duration,
    batch_size: usize,
) -> Result<usize, String> {
    if batch_size == 0 {
        return Ok(0);
    }

    let cutoff = Utc::now()
        - chrono::Duration::seconds(i64::try_from(ttl.as_secs()).unwrap_or(0));

    let mut total_removed = 0usize;

    loop {
        let ids = repository
            .finality_status_repository()
            .find_ids_older_than(cutoff, batch_size as u64)
            .await
            .map_err(|e| e.to_string())?;

        if ids.is_empty() {
            break;
        }

        let removed_rows = repository
            .finality_status_repository()
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
