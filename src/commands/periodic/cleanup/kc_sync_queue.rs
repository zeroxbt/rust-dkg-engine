use std::time::Duration;

use chrono::Utc;

use crate::managers::repository::RepositoryManager;

pub(crate) async fn cleanup_kc_sync_queue(
    repository: &RepositoryManager,
    ttl: Duration,
    max_retries: u32,
    batch_size: usize,
) -> Result<usize, String> {
    if batch_size == 0 {
        return Ok(0);
    }

    let cutoff = Utc::now().timestamp().saturating_sub(ttl.as_secs() as i64);

    let mut total_removed = 0usize;

    loop {
        let entries = repository
            .kc_sync_repository()
            .find_stale_entries(max_retries, cutoff, batch_size as u64)
            .await
            .map_err(|e| e.to_string())?;

        if entries.is_empty() {
            break;
        }

        let removed_rows = repository
            .kc_sync_repository()
            .delete_entries(&entries)
            .await
            .map_err(|e| e.to_string())?;

        total_removed = total_removed.saturating_add(removed_rows as usize);

        if entries.len() < batch_size {
            break;
        }
    }

    Ok(total_removed)
}
