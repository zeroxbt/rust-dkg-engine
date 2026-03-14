use std::time::Duration;

use dkg_key_value_store::{KeyValueStoreError, PublishTmpDatasetStore};

pub(crate) async fn cleanup_publish_tmp_datasets(
    publish_tmp_dataset: &PublishTmpDatasetStore,
    ttl: Duration,
    batch_size: usize,
) -> Result<usize, KeyValueStoreError> {
    if batch_size == 0 {
        return Ok(0);
    }

    let mut total_removed = 0usize;

    loop {
        let removed = publish_tmp_dataset.remove_expired(ttl, batch_size).await?;

        total_removed = total_removed.saturating_add(removed);

        if removed < batch_size {
            break;
        }
    }

    Ok(total_removed)
}
