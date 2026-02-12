use std::time::Duration;

use chrono::Utc;

use crate::managers::repository::RepositoryManager;

pub(crate) async fn cleanup_proof_challenges(
    repository: &RepositoryManager,
    ttl: Duration,
    batch_size: usize,
) -> Result<usize, String> {
    if batch_size == 0 {
        return Ok(0);
    }

    let cutoff = Utc::now().timestamp().saturating_sub(ttl.as_secs() as i64);

    let mut total_removed = 0usize;

    loop {
        let entries = repository
            .proof_challenge_repository()
            .find_expired(cutoff, batch_size as u64)
            .await
            .map_err(|e| e.to_string())?;

        if entries.is_empty() {
            break;
        }

        let removed_rows = repository
            .proof_challenge_repository()
            .delete_by_keys(&entries)
            .await
            .map_err(|e| e.to_string())?;

        total_removed = total_removed.saturating_add(removed_rows as usize);

        if entries.len() < batch_size {
            break;
        }
    }

    Ok(total_removed)
}
