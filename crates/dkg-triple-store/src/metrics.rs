use std::time::Duration;

use dkg_domain::KnowledgeAsset;
use dkg_observability as observability;

use crate::error::TripleStoreError;

pub(crate) struct KcInsertCharacteristics {
    pub(crate) raw_bytes: usize,
    pub(crate) ka_count: usize,
    pub(crate) has_private: bool,
    pub(crate) has_metadata: bool,
    pub(crate) has_paranet: bool,
}

impl KcInsertCharacteristics {
    pub(crate) fn from_assets(
        knowledge_assets: &[KnowledgeAsset],
        has_metadata: bool,
        has_paranet: bool,
    ) -> Self {
        let mut raw_bytes = 0usize;
        let mut has_private = false;

        for asset in knowledge_assets {
            raw_bytes += joined_lines_bytes(asset.public_triples());
            if let Some(private) = asset.private_triples()
                && !private.is_empty()
            {
                has_private = true;
                raw_bytes += joined_lines_bytes(private);
            }
        }

        Self {
            raw_bytes,
            ka_count: knowledge_assets.len(),
            has_private,
            has_metadata,
            has_paranet,
        }
    }
}

pub(crate) fn record_backend_query_bytes_total(backend: &str, op: &str, bytes: usize) {
    observability::record_triple_store_backend_query_bytes_total(backend, op, bytes);
}

pub(crate) fn record_backend_result_bytes_total(backend: &str, op: &str, bytes: usize) {
    observability::record_triple_store_backend_result_bytes_total(backend, op, bytes);
}

pub(crate) fn record_backend_permit_wait(backend: &str, op: &str, wait: Duration) {
    observability::record_triple_store_backend_permit_wait(backend, op, wait);
}

pub(crate) fn record_backend_permit_snapshot(backend: &str, max: usize, available: usize) {
    observability::record_triple_store_backend_permit_snapshot(backend, max, available);
}

pub(crate) fn record_backend_operation(
    backend: &str,
    op: &str,
    error: Option<&TripleStoreError>,
    duration: Duration,
) {
    let status = if error.is_some() { "error" } else { "ok" };
    let error_class = error.map_or("none", classify_error);

    observability::record_triple_store_backend_operation(
        backend,
        op,
        status,
        error_class,
        duration,
    );
}

pub(crate) fn record_kc_insert(
    backend: &str,
    insert: &KcInsertCharacteristics,
    inserted_triples: usize,
    error: Option<&TripleStoreError>,
    duration: Duration,
) {
    let status = if error.is_some() { "error" } else { "ok" };
    let error_class = error.map_or("none", classify_error);
    let size_bucket = bytes_bucket(insert.raw_bytes);
    let ka_bucket = ka_count_bucket(insert.ka_count);
    let triples_bucket = triples_count_bucket(inserted_triples);

    observability::record_triple_store_kc_insert(
        backend,
        status,
        error_class,
        size_bucket,
        ka_bucket,
        triples_bucket,
        insert.has_private,
        insert.has_metadata,
        insert.has_paranet,
        duration,
        insert.raw_bytes,
        inserted_triples,
    );
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record_query_operation(
    backend: &str,
    query_kind: &str,
    visibility: &str,
    error: Option<&TripleStoreError>,
    duration: Duration,
    result_bytes: usize,
    result_triples: usize,
    asset_count: Option<u64>,
    requested_uals: Option<usize>,
) {
    let status = if error.is_some() { "error" } else { "ok" };
    let error_class = error.map_or("none", classify_error);
    let asset_bucket = asset_count.map_or("n/a", asset_count_bucket);
    let requested_uals_bucket = requested_uals.map_or("n/a", requested_uals_bucket);

    observability::record_triple_store_query_operation(
        backend,
        query_kind,
        visibility,
        status,
        error_class,
        asset_bucket,
        requested_uals_bucket,
        duration,
        if error.is_none() {
            Some(result_bytes)
        } else {
            None
        },
        if error.is_none() {
            Some(result_triples)
        } else {
            None
        },
    );
}

fn joined_lines_bytes(lines: &[String]) -> usize {
    if lines.is_empty() {
        return 0;
    }
    lines.iter().map(String::len).sum::<usize>() + lines.len().saturating_sub(1)
}

fn classify_error(error: &TripleStoreError) -> &'static str {
    match error {
        TripleStoreError::SemaphoreClosed => "semaphore_closed",
        TripleStoreError::Http(_) => "http",
        TripleStoreError::Io(_) => "io",
        TripleStoreError::Backend { status, .. } if *status >= 500 => "backend_5xx",
        TripleStoreError::Backend { status, .. } if *status >= 400 => "backend_4xx",
        TripleStoreError::Backend { .. } => "backend_other",
        TripleStoreError::ConnectionFailed { .. } => "connection_failed",
        TripleStoreError::ParseError { .. } => "parse_error",
        TripleStoreError::InvalidQuery { .. } => "invalid_query",
        TripleStoreError::Other(_) => "other",
    }
}

fn bytes_bucket(bytes: usize) -> &'static str {
    match bytes {
        0..=16_384 => "<=16KiB",
        16_385..=65_536 => "16-64KiB",
        65_537..=262_144 => "64-256KiB",
        262_145..=1_048_576 => "256KiB-1MiB",
        _ => ">1MiB",
    }
}

fn ka_count_bucket(ka_count: usize) -> &'static str {
    match ka_count {
        0 | 1 => "1",
        2..=5 => "2-5",
        6..=20 => "6-20",
        _ => ">20",
    }
}

fn triples_count_bucket(triples: usize) -> &'static str {
    match triples {
        0..=100 => "<=100",
        101..=500 => "101-500",
        501..=2_000 => "501-2k",
        2_001..=10_000 => "2k-10k",
        _ => ">10k",
    }
}

fn asset_count_bucket(asset_count: u64) -> &'static str {
    match asset_count {
        0..=1 => "1",
        2..=5 => "2-5",
        6..=20 => "6-20",
        21..=100 => "21-100",
        _ => ">100",
    }
}

fn requested_uals_bucket(count: usize) -> &'static str {
    match count {
        0..=1 => "1",
        2..=10 => "2-10",
        11..=50 => "11-50",
        51..=200 => "51-200",
        _ => ">200",
    }
}
