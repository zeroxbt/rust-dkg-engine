use std::time::Duration;

use crate::services::PendingStorageService;

pub(crate) fn cleanup_pending_storage(
    pending_storage: &PendingStorageService,
    ttl: Duration,
    batch_size: usize,
) -> Result<usize, String> {
    if batch_size == 0 {
        return Ok(0);
    }

    let mut total_removed = 0usize;

    loop {
        let removed = pending_storage
            .remove_expired(ttl, batch_size)
            .map_err(|e| e.to_string())?;

        total_removed = total_removed.saturating_add(removed);

        if removed < batch_size {
            break;
        }
    }

    Ok(total_removed)
}
