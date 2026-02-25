use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use clap::{Parser, ValueEnum};
use dkg_triple_store::{
    GraphVisibility, OxigraphStoreConfig, TimeoutConfig, TripleStoreBackendType,
    TripleStoreManager, TripleStoreManagerConfig,
};
use futures::{StreamExt, stream};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
struct WorkItem {
    kc_ual: String,
    start_token_id: u64,
    end_token_id: u64,
    #[serde(default)]
    burned: Vec<u64>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum MetadataMode {
    None,
    Batch,
    PerItem,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum MetadataScope {
    All,
    Core,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum VisibilityArg {
    Public,
    Private,
    All,
}

#[derive(Debug, Parser)]
#[command(name = "query-probe")]
#[command(about = "Standalone Oxigraph query and memory probe")]
struct Args {
    /// Path to triple-store data root (the directory that contains DKG/)
    #[arg(long)]
    store_path: PathBuf,

    /// Workload file path (JSON array or JSONL of WorkItem)
    #[arg(long)]
    workload: PathBuf,

    /// Number of iterations over the workload
    #[arg(long, default_value_t = 1)]
    iterations: usize,

    /// Workload chunk size for each phase call
    #[arg(long, default_value_t = 50)]
    batch_size: usize,

    /// Delay between iterations
    #[arg(long, default_value_t = 0)]
    sleep_ms: u64,

    /// Whether to fetch collection data after existence check
    #[arg(long, default_value_t = false)]
    fetch_data: bool,

    /// Graph visibility for collection fetches
    #[arg(long, value_enum, default_value_t = VisibilityArg::Public)]
    visibility: VisibilityArg,

    /// Concurrency for collection fetches in probe (not triple-store internal semaphore)
    #[arg(long, default_value_t = 4)]
    fetch_concurrency: usize,

    /// Metadata mode after existence check
    #[arg(long, value_enum, default_value_t = MetadataMode::Batch)]
    metadata_mode: MetadataMode,

    /// Metadata query scope: `all` (current behavior) or `core` (publish-related predicates only)
    #[arg(long, value_enum, default_value_t = MetadataScope::All)]
    metadata_scope: MetadataScope,

    /// Triple-store manager max concurrent operations
    #[arg(long, default_value_t = 16)]
    max_concurrent_operations: usize,

    /// Triple-store query timeout (milliseconds)
    #[arg(long, default_value_t = 60_000)]
    query_timeout_ms: u64,

    /// Triple-store ask timeout (milliseconds)
    #[arg(long, default_value_t = 10_000)]
    ask_timeout_ms: u64,

    /// Triple-store insert timeout (milliseconds)
    #[arg(long, default_value_t = 300_000)]
    insert_timeout_ms: u64,

    /// Oxigraph max_open_files
    #[arg(long)]
    oxigraph_max_open_files: Option<u32>,

    /// Oxigraph fd_reserve
    #[arg(long)]
    oxigraph_fd_reserve: Option<u32>,
}

#[derive(Debug, Serialize)]
struct PhaseLog {
    ts_unix_ms: u128,
    phase: String,
    iteration: usize,
    batch_index: usize,
    batch_size: usize,
    elapsed_ms: u128,
    rss_before_bytes: Option<u64>,
    rss_after_bytes: Option<u64>,
    rss_delta_bytes: Option<i128>,
    fds_before: Option<u64>,
    fds_after: Option<u64>,
    fds_delta: Option<i128>,
    requested_kc_count: usize,
    existing_kc_count: usize,
    metadata_kc_hit_count: Option<usize>,
    metadata_kc_miss_count: Option<usize>,
    triples_count: Option<usize>,
    payload_bytes: Option<usize>,
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis()
}

fn read_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let vmrss_line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kb = vmrss_line
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse::<u64>().ok())?;
    Some(kb.saturating_mul(1024))
}

fn read_fd_count() -> Option<u64> {
    let count = fs::read_dir("/proc/self/fd").ok()?.count();
    Some(count as u64)
}

fn signed_delta(after: Option<u64>, before: Option<u64>) -> Option<i128> {
    match (after, before) {
        (Some(a), Some(b)) => Some((a as i128) - (b as i128)),
        _ => None,
    }
}

fn emit_log(mut log: PhaseLog) {
    log.ts_unix_ms = now_unix_ms();
    match serde_json::to_string(&log) {
        Ok(line) => println!("{line}"),
        Err(err) => eprintln!("failed to serialize log line: {err}"),
    }
}

fn parse_workload(path: &Path) -> Result<Vec<WorkItem>, String> {
    let raw =
        fs::read_to_string(path).map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    let trimmed = raw.trim_start();
    if trimmed.starts_with('[') {
        let items: Vec<WorkItem> = serde_json::from_str(&raw)
            .map_err(|e| format!("failed to parse JSON array workload: {e}"))?;
        return Ok(items);
    }

    let mut items = Vec::new();
    for (idx, line) in raw.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let item: WorkItem = serde_json::from_str(line)
            .map_err(|e| format!("invalid JSONL at line {}: {e}", idx + 1))?;
        items.push(item);
    }
    Ok(items)
}

fn payload_bytes(lines: &[String]) -> usize {
    if lines.is_empty() {
        return 0;
    }
    lines.iter().map(String::len).sum::<usize>() + lines.len().saturating_sub(1)
}

fn payload_bytes_map(values: &std::collections::HashMap<String, Vec<String>>) -> usize {
    values.values().map(|lines| payload_bytes(lines)).sum()
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let args = Args::parse();
    let workload = parse_workload(&args.workload)?;
    if workload.is_empty() {
        return Err("workload is empty".to_string());
    }

    let config = TripleStoreManagerConfig {
        backend: TripleStoreBackendType::Oxigraph,
        url: String::new(),
        username: None,
        password: None,
        connect_max_retries: 1,
        connect_retry_frequency_ms: 1000,
        timeouts: TimeoutConfig {
            query_ms: args.query_timeout_ms,
            insert_ms: args.insert_timeout_ms,
            ask_ms: args.ask_timeout_ms,
        },
        max_concurrent_operations: args.max_concurrent_operations,
        oxigraph: OxigraphStoreConfig {
            max_open_files: args.oxigraph_max_open_files,
            fd_reserve: args.oxigraph_fd_reserve,
        },
    };

    eprintln!(
        "connecting store_path={} workload_items={} iterations={} batch_size={} metadata_mode={:?} metadata_scope={:?}",
        args.store_path.display(),
        workload.len(),
        args.iterations,
        args.batch_size,
        args.metadata_mode,
        args.metadata_scope
    );

    let manager = Arc::new(
        TripleStoreManager::connect(&config, &args.store_path)
            .await
            .map_err(|e| format!("failed to connect triple store: {e}"))?,
    );

    let mut batches = Vec::new();
    let effective_batch_size = args.batch_size.max(1);
    for chunk in workload.chunks(effective_batch_size) {
        batches.push(chunk.to_vec());
    }

    for iteration in 0..args.iterations {
        for (batch_index, batch) in batches.iter().enumerate() {
            let requested_kc_count = batch.len();
            let boundaries: Vec<(String, u64, u64)> = batch
                .iter()
                .filter_map(|item| {
                    if item.start_token_id > item.end_token_id {
                        return None;
                    }

                    let burned_set: HashSet<u64> = item.burned.iter().copied().collect();
                    let mut first = item.start_token_id;
                    while first <= item.end_token_id && burned_set.contains(&first) {
                        first = first.saturating_add(1);
                    }
                    if first > item.end_token_id {
                        return None;
                    }

                    let mut last = item.end_token_id;
                    while last > first && burned_set.contains(&last) {
                        last = last.saturating_sub(1);
                    }
                    if burned_set.contains(&last) {
                        return None;
                    }

                    Some((item.kc_ual.clone(), first, last))
                })
                .collect();

            let rss_before = read_rss_bytes();
            let fds_before = read_fd_count();
            let started = Instant::now();

            let existing_kcs = manager
                .knowledge_collections_exist_by_boundary_graphs(&boundaries)
                .await
                .map_err(|e| format!("boundary check failed at iteration={iteration}: {e}"))?;

            let elapsed_ms = started.elapsed().as_millis();
            let rss_after = read_rss_bytes();
            let fds_after = read_fd_count();

            emit_log(PhaseLog {
                ts_unix_ms: 0,
                phase: "boundary_check".to_string(),
                iteration,
                batch_index,
                batch_size: requested_kc_count,
                elapsed_ms,
                rss_before_bytes: rss_before,
                rss_after_bytes: rss_after,
                rss_delta_bytes: signed_delta(rss_after, rss_before),
                fds_before,
                fds_after,
                fds_delta: signed_delta(fds_after, fds_before),
                requested_kc_count,
                existing_kc_count: existing_kcs.len(),
                metadata_kc_hit_count: None,
                metadata_kc_miss_count: None,
                triples_count: None,
                payload_bytes: None,
            });

            if existing_kcs.is_empty() {
                continue;
            }

            let existing_items: Vec<&WorkItem> = batch
                .iter()
                .filter(|item| existing_kcs.contains(&item.kc_ual))
                .collect();

            if args.fetch_data {
                let rss_before = read_rss_bytes();
                let fds_before = read_fd_count();
                let started = Instant::now();
                let visibility = args.visibility;
                let manager_for_fetch = Arc::clone(&manager);

                let mut triples_count = 0usize;
                let mut total_bytes = 0usize;

                let mut fetches = stream::iter(existing_items.iter())
                    .map(move |item| {
                        let manager = Arc::clone(&manager_for_fetch);
                        async move {
                            match visibility {
                                VisibilityArg::Public => {
                                    manager
                                        .get_knowledge_collection_named_graphs(
                                            &item.kc_ual,
                                            item.start_token_id,
                                            item.end_token_id,
                                            &item.burned,
                                            GraphVisibility::Public,
                                        )
                                        .await
                                }
                                VisibilityArg::Private => {
                                    manager
                                        .get_knowledge_collection_named_graphs(
                                            &item.kc_ual,
                                            item.start_token_id,
                                            item.end_token_id,
                                            &item.burned,
                                            GraphVisibility::Private,
                                        )
                                        .await
                                }
                                VisibilityArg::All => {
                                    let manager_private = Arc::clone(&manager);
                                    let (public, private) = tokio::join!(
                                        manager.get_knowledge_collection_named_graphs(
                                            &item.kc_ual,
                                            item.start_token_id,
                                            item.end_token_id,
                                            &item.burned,
                                            GraphVisibility::Public
                                        ),
                                        manager_private.get_knowledge_collection_named_graphs(
                                            &item.kc_ual,
                                            item.start_token_id,
                                            item.end_token_id,
                                            &item.burned,
                                            GraphVisibility::Private
                                        )
                                    );
                                    let mut public = public?;
                                    let private = private?;
                                    public.extend(private);
                                    Ok(public)
                                }
                            }
                        }
                    })
                    .buffer_unordered(args.fetch_concurrency.max(1));

                while let Some(result) = fetches.next().await {
                    let lines = result.map_err(|e| {
                        format!("collection fetch failed at iteration={iteration}: {e}")
                    })?;
                    triples_count = triples_count.saturating_add(lines.len());
                    total_bytes = total_bytes.saturating_add(payload_bytes(&lines));
                }

                let elapsed_ms = started.elapsed().as_millis();
                let rss_after = read_rss_bytes();
                let fds_after = read_fd_count();

                emit_log(PhaseLog {
                    ts_unix_ms: 0,
                    phase: "fetch_data".to_string(),
                    iteration,
                    batch_index,
                    batch_size: requested_kc_count,
                    elapsed_ms,
                    rss_before_bytes: rss_before,
                    rss_after_bytes: rss_after,
                    rss_delta_bytes: signed_delta(rss_after, rss_before),
                    fds_before,
                    fds_after,
                    fds_delta: signed_delta(fds_after, fds_before),
                    requested_kc_count,
                    existing_kc_count: existing_items.len(),
                    metadata_kc_hit_count: None,
                    metadata_kc_miss_count: None,
                    triples_count: Some(triples_count),
                    payload_bytes: Some(total_bytes),
                });
            }

            match args.metadata_mode {
                MetadataMode::None => {}
                MetadataMode::Batch => {
                    let kc_uals: Vec<String> = existing_items
                        .iter()
                        .map(|item| item.kc_ual.clone())
                        .collect();

                    let rss_before = read_rss_bytes();
                    let fds_before = read_fd_count();
                    let started = Instant::now();

                    let metadata = match args.metadata_scope {
                        MetadataScope::All => manager.get_metadata_batch(&kc_uals).await,
                        MetadataScope::Core => manager.get_metadata_core_batch(&kc_uals).await,
                    }
                    .map_err(|e| format!("metadata batch failed at iteration={iteration}: {e}"))?;

                    let elapsed_ms = started.elapsed().as_millis();
                    let rss_after = read_rss_bytes();
                    let fds_after = read_fd_count();
                    let triples_count = metadata.values().map(Vec::len).sum::<usize>();
                    let total_bytes = payload_bytes_map(&metadata);
                    let metadata_kc_hit_count = metadata.len();
                    let metadata_kc_miss_count =
                        existing_items.len().saturating_sub(metadata_kc_hit_count);

                    emit_log(PhaseLog {
                        ts_unix_ms: 0,
                        phase: "metadata_batch".to_string(),
                        iteration,
                        batch_index,
                        batch_size: requested_kc_count,
                        elapsed_ms,
                        rss_before_bytes: rss_before,
                        rss_after_bytes: rss_after,
                        rss_delta_bytes: signed_delta(rss_after, rss_before),
                        fds_before,
                        fds_after,
                        fds_delta: signed_delta(fds_after, fds_before),
                        requested_kc_count,
                        existing_kc_count: existing_items.len(),
                        metadata_kc_hit_count: Some(metadata_kc_hit_count),
                        metadata_kc_miss_count: Some(metadata_kc_miss_count),
                        triples_count: Some(triples_count),
                        payload_bytes: Some(total_bytes),
                    });
                }
                MetadataMode::PerItem => {
                    let rss_before = read_rss_bytes();
                    let fds_before = read_fd_count();
                    let started = Instant::now();

                    let mut triples_count = 0usize;
                    let mut total_bytes = 0usize;
                    let mut metadata_kc_hit_count = 0usize;

                    for item in &existing_items {
                        let metadata = match args.metadata_scope {
                            MetadataScope::All => manager.get_metadata(&item.kc_ual).await,
                            MetadataScope::Core => manager.get_metadata_core(&item.kc_ual).await,
                        }
                        .map_err(|e| {
                            format!("metadata per-item failed at iteration={iteration}: {e}")
                        })?;
                        let lines: Vec<String> = metadata
                            .lines()
                            .filter(|line| !line.trim().is_empty())
                            .map(str::to_string)
                            .collect();
                        if !lines.is_empty() {
                            metadata_kc_hit_count += 1;
                        }
                        triples_count = triples_count.saturating_add(lines.len());
                        total_bytes = total_bytes.saturating_add(payload_bytes(&lines));
                    }

                    let elapsed_ms = started.elapsed().as_millis();
                    let rss_after = read_rss_bytes();
                    let fds_after = read_fd_count();

                    emit_log(PhaseLog {
                        ts_unix_ms: 0,
                        phase: "metadata_per_item".to_string(),
                        iteration,
                        batch_index,
                        batch_size: requested_kc_count,
                        elapsed_ms,
                        rss_before_bytes: rss_before,
                        rss_after_bytes: rss_after,
                        rss_delta_bytes: signed_delta(rss_after, rss_before),
                        fds_before,
                        fds_after,
                        fds_delta: signed_delta(fds_after, fds_before),
                        requested_kc_count,
                        existing_kc_count: existing_items.len(),
                        metadata_kc_hit_count: Some(metadata_kc_hit_count),
                        metadata_kc_miss_count: Some(
                            existing_items.len().saturating_sub(metadata_kc_hit_count),
                        ),
                        triples_count: Some(triples_count),
                        payload_bytes: Some(total_bytes),
                    });
                }
            }
        }

        if args.sleep_ms > 0 && iteration + 1 < args.iterations {
            tokio::time::sleep(Duration::from_millis(args.sleep_ms)).await;
        }
    }

    Ok(())
}
