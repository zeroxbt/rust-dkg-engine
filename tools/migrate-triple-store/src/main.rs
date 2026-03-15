use std::{collections::HashSet, path::Path, time::Instant};

use alloy::primitives::Address;
use clap::Parser;
use dkg_domain::{BlockchainId, KnowledgeAsset, KnowledgeCollectionMetadata, derive_ual};
use dkg_repository::{
    KcChainReadyKcStateMetadataEntry, RepositoryManager, RepositoryManagerConfig,
};
use dkg_triple_store::{
    GraphVisibility, OxigraphStoreConfig, TimeoutConfig, TripleStoreBackendType,
    TripleStoreManager, TripleStoreManagerConfig,
};

/// Migrate triple store data between backends (Oxigraph <-> Blazegraph).
///
/// Reads KC IDs from the MySQL metadata table, then for each KC:
/// 1. Reads metadata + data graphs from the source triple store
/// 2. Inserts them into the destination via insert_knowledge_collection
#[derive(Parser, Debug)]
#[command(name = "migrate-triple-store")]
struct Args {
    // ---- Source backend ----
    /// Source backend type: "oxigraph" or "blazegraph"
    #[arg(long)]
    source_backend: Backend,

    /// Source Oxigraph data directory (parent of "DKG" subdirectory).
    /// Required when source-backend is oxigraph.
    #[arg(long)]
    source_oxigraph_path: Option<String>,

    /// Source Blazegraph base URL (e.g. http://localhost:9999).
    /// Required when source-backend is blazegraph.
    #[arg(long)]
    source_blazegraph_url: Option<String>,

    // ---- Destination backend ----
    /// Destination backend type: "oxigraph" or "blazegraph"
    #[arg(long)]
    dest_backend: Backend,

    /// Destination Oxigraph data directory (parent of "DKG" subdirectory).
    /// Required when dest-backend is oxigraph.
    #[arg(long)]
    dest_oxigraph_path: Option<String>,

    /// Destination Blazegraph base URL (e.g. http://localhost:9999).
    /// Required when dest-backend is blazegraph.
    #[arg(long)]
    dest_blazegraph_url: Option<String>,

    // ---- MySQL ----
    /// MySQL host
    #[arg(long, default_value = "localhost")]
    db_host: String,

    /// MySQL port
    #[arg(long, default_value = "3306")]
    db_port: u16,

    /// MySQL database name
    #[arg(long)]
    db_name: String,

    /// MySQL user
    #[arg(long)]
    db_user: String,

    /// MySQL password
    #[arg(long)]
    db_password: String,

    // ---- KC selection ----
    /// Blockchain ID to filter KCs (e.g. "otp:2043")
    #[arg(long)]
    blockchain_id: String,

    /// Contract address to filter KCs
    #[arg(long)]
    contract_address: String,

    /// Skip KCs that already exist in the destination
    #[arg(long, default_value = "true")]
    skip_existing: bool,

    /// Start from this KC ID (useful for resuming)
    #[arg(long, default_value = "0")]
    start_from_kc_id: u64,

    /// Process KCs in chunks of this size (for fetching metadata from MySQL)
    #[arg(long, default_value = "500")]
    chunk_size: u64,

    /// Dry run: count triples without inserting into destination
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum Backend {
    Oxigraph,
    Blazegraph,
}

fn build_manager_config(
    backend: &Backend,
    oxigraph_path: &Option<String>,
    blazegraph_url: &Option<String>,
) -> anyhow::Result<(TripleStoreManagerConfig, String)> {
    match backend {
        Backend::Oxigraph => {
            let path = oxigraph_path.as_ref().ok_or_else(|| {
                anyhow::anyhow!("--*-oxigraph-path is required for oxigraph backend")
            })?;
            let config = TripleStoreManagerConfig {
                backend: TripleStoreBackendType::Oxigraph,
                url: String::new(),
                username: None,
                password: None,
                connect_max_retries: 0,
                connect_retry_frequency_ms: 0,
                timeouts: TimeoutConfig {
                    query_ms: 120_000,
                    insert_ms: 600_000,
                    ask_ms: 30_000,
                },
                max_concurrent_operations: 1,
                collection_fetch_page_concurrency: 1,
                collection_fetch_max_token_ids_per_page: 50,
                oxigraph: OxigraphStoreConfig::default(),
            };
            Ok((config, path.clone()))
        }
        Backend::Blazegraph => {
            let url = blazegraph_url.as_ref().ok_or_else(|| {
                anyhow::anyhow!("--*-blazegraph-url is required for blazegraph backend")
            })?;
            let config = TripleStoreManagerConfig {
                backend: TripleStoreBackendType::Blazegraph,
                url: url.clone(),
                username: None,
                password: None,
                connect_max_retries: 3,
                connect_retry_frequency_ms: 2_000,
                timeouts: TimeoutConfig {
                    query_ms: 120_000,
                    insert_ms: 600_000,
                    ask_ms: 30_000,
                },
                max_concurrent_operations: 1,
                collection_fetch_page_concurrency: 1,
                collection_fetch_max_token_ids_per_page: 50,
                oxigraph: OxigraphStoreConfig::default(),
            };
            Ok((config, String::new()))
        }
    }
}

fn backend_label(backend: &Backend, path: &Option<String>, url: &Option<String>) -> String {
    match backend {
        Backend::Oxigraph => format!("oxigraph ({})", path.as_deref().unwrap_or("?")),
        Backend::Blazegraph => format!("blazegraph ({})", url.as_deref().unwrap_or("?")),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let source_label = backend_label(
        &args.source_backend,
        &args.source_oxigraph_path,
        &args.source_blazegraph_url,
    );
    let dest_label = backend_label(
        &args.dest_backend,
        &args.dest_oxigraph_path,
        &args.dest_blazegraph_url,
    );
    println!("Migration: {} -> {}", source_label, dest_label);

    // --- Source ---
    let (source_config, source_path) = build_manager_config(
        &args.source_backend,
        &args.source_oxigraph_path,
        &args.source_blazegraph_url,
    )?;
    println!("Connecting to source ({})...", source_label);
    let source = TripleStoreManager::connect(&source_config, Path::new(&source_path)).await?;
    println!("Source ready");

    // --- Destination ---
    let dest = if args.dry_run {
        None
    } else {
        let (dest_config, dest_path) = build_manager_config(
            &args.dest_backend,
            &args.dest_oxigraph_path,
            &args.dest_blazegraph_url,
        )?;
        println!("Connecting to destination ({})...", dest_label);
        let d = TripleStoreManager::connect(&dest_config, Path::new(&dest_path)).await?;
        println!("Destination ready");
        Some(d)
    };

    // --- MySQL: enumerate KCs ---
    println!(
        "Connecting to MySQL {}:{}/{}...",
        args.db_host, args.db_port, args.db_name
    );
    let repo_config = RepositoryManagerConfig {
        host: args.db_host.clone(),
        port: args.db_port,
        database: args.db_name.clone(),
        user: args.db_user.clone(),
        password: args.db_password.clone(),
        max_connections: 2,
        min_connections: 1,
    };
    let repo = RepositoryManager::connect(&repo_config).await?;
    let kc_meta_repo = repo.kc_chain_metadata_repository();
    println!("MySQL connected");

    // Parse blockchain/contract for UAL derivation
    let blockchain = args
        .blockchain_id
        .parse::<BlockchainId>()
        .map_err(|error| {
            anyhow::anyhow!("Invalid blockchain id {}: {}", args.blockchain_id, error)
        })?;
    let contract: Address = args
        .contract_address
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid contract address: {}", args.contract_address))?;

    // Get total KC count for progress reporting
    let total_kcs = kc_meta_repo
        .count_core_metadata_for_blockchain(&args.blockchain_id)
        .await?;
    println!(
        "Total KCs in database for {}: {} (starting from kc_id={})",
        args.blockchain_id, total_kcs, args.start_from_kc_id
    );

    let mut migrated = 0u64;
    let mut skipped_existing = 0u64;
    let mut skipped_empty = 0u64;
    let mut failed = 0u64;
    let mut total_triples = 0u64;
    let mut processed = 0u64;
    let overall_start = Instant::now();

    let mut current_start_id = args.start_from_kc_id;
    let mut consecutive_empty_chunks = 0u32;

    loop {
        let kc_ids: Vec<u64> = (current_start_id..current_start_id + args.chunk_size).collect();

        let entries = kc_meta_repo
            .get_many_ready_with_kc_state_metadata(
                &args.blockchain_id,
                &args.contract_address,
                &kc_ids,
            )
            .await?;

        if entries.is_empty() {
            consecutive_empty_chunks += 1;
            if consecutive_empty_chunks >= 20 {
                break;
            }
            current_start_id += args.chunk_size;
            continue;
        }

        consecutive_empty_chunks = 0;

        let mut sorted_entries: Vec<_> = entries.into_iter().collect();
        sorted_entries.sort_by_key(|(kc_id, _)| *kc_id);

        for (kc_id, entry) in &sorted_entries {
            processed += 1;
            let kc_ual = derive_ual(&blockchain, &contract, *kc_id as u128, None);
            let kc_start = Instant::now();

            // Skip if already in destination
            if args.skip_existing
                && !args.dry_run
                && let Some(dest) = &dest
            {
                match kc_exists_in_dest(dest, &kc_ual).await {
                    Ok(true) => {
                        skipped_existing += 1;
                        if processed.is_multiple_of(100) {
                            print_progress(
                                processed,
                                total_kcs,
                                migrated,
                                skipped_existing,
                                skipped_empty,
                                failed,
                                &overall_start,
                            );
                        }
                        continue;
                    }
                    Ok(false) => {}
                    Err(e) => {
                        eprintln!("[KC {}] Warning: existence check failed: {}", kc_id, e);
                    }
                }
            }

            match migrate_single_kc(
                &source,
                dest.as_ref(),
                &blockchain,
                &contract,
                entry,
                args.dry_run,
            )
            .await
            {
                Ok(0) => {
                    skipped_empty += 1;
                }
                Ok(count) => {
                    migrated += 1;
                    total_triples += count as u64;
                    let elapsed = kc_start.elapsed();
                    if processed.is_multiple_of(10) || elapsed.as_secs() > 5 {
                        println!(
                            "[KC {} | {}/~{}] {} triples in {:.1}s{}",
                            kc_id,
                            processed,
                            total_kcs,
                            count,
                            elapsed.as_secs_f64(),
                            if args.dry_run { " (dry run)" } else { "" },
                        );
                    }
                }
                Err(e) => {
                    eprintln!("[KC {}] Error: {}", kc_id, e);
                    failed += 1;
                }
            }
        }

        current_start_id += args.chunk_size;
    }

    let total_elapsed = overall_start.elapsed();
    println!("\n=== Migration complete ===");
    println!("Direction:       {} -> {}", source_label, dest_label);
    println!("Processed:       {}", processed);
    println!("Migrated:        {}", migrated);
    println!("Skipped (exist): {}", skipped_existing);
    println!("Skipped (empty): {}", skipped_empty);
    println!("Failed:          {}", failed);
    println!("Total triples:   {}", total_triples);
    println!("Elapsed:         {:.1}s", total_elapsed.as_secs_f64());

    if failed > 0 {
        eprintln!(
            "\n{} KCs failed. Re-run with --start-from-kc-id to resume.",
            failed
        );
    }

    Ok(())
}

/// Check if a KC already exists in the destination triple store.
async fn kc_exists_in_dest(dest: &TripleStoreManager, kc_ual: &str) -> anyhow::Result<bool> {
    let metadata = dest.get_metadata(kc_ual).await?;
    Ok(!metadata.trim().is_empty())
}

/// Migrate a single KC from source to destination.
///
/// Reads data graphs from source, builds metadata from the SQL entry, and uses
/// `insert_knowledge_collection` on the destination for a faithful insert.
///
/// Returns the number of triples migrated (0 if KC has no data in source).
async fn migrate_single_kc(
    source: &TripleStoreManager,
    dest: Option<&TripleStoreManager>,
    blockchain: &BlockchainId,
    contract: &Address,
    entry: &KcChainReadyKcStateMetadataEntry,
    dry_run: bool,
) -> anyhow::Result<usize> {
    let kc_ual = derive_ual(blockchain, contract, entry.kc_id as u128, None);

    // 1. Build metadata from the SQL entry (source of truth)
    let metadata = KnowledgeCollectionMetadata::new(
        entry.publisher_address.clone(),
        entry.block_number,
        entry.transaction_hash.clone(),
        entry.block_timestamp,
    );

    // 2. Decode burned token IDs so we skip them
    let burned_set: HashSet<u64> = decode_burned_ids(
        entry.burned_mode,
        &entry.burned_payload,
        entry.range_start_token_id,
        entry.range_end_token_id,
    )
    .into_iter()
    .collect();

    // 3. Build KnowledgeAsset objects for each non-burned token in the range
    let mut knowledge_assets: Vec<KnowledgeAsset> = Vec::new();
    let mut total_data_triples = 0usize;

    for token_id in entry.range_start_token_id..=entry.range_end_token_id {
        if burned_set.contains(&token_id) {
            continue;
        }
        let ka_ual = derive_ual(
            blockchain,
            contract,
            entry.kc_id as u128,
            Some(token_id as u128),
        );

        let public_triples = source
            .get_knowledge_asset_named_graph(&ka_ual, GraphVisibility::Public)
            .await?;

        if public_triples.is_empty() {
            continue;
        }

        total_data_triples += public_triples.len();
        let mut ka = KnowledgeAsset::new(ka_ual, public_triples);

        let private_triples = source
            .get_knowledge_asset_named_graph(ka.ual(), GraphVisibility::Private)
            .await?;

        if !private_triples.is_empty() {
            total_data_triples += private_triples.len();
            ka.set_private_triples(private_triples);
        }

        knowledge_assets.push(ka);
    }

    if total_data_triples == 0 {
        return Ok(0);
    }

    if dry_run {
        return Ok(total_data_triples);
    }

    let dest = dest.ok_or_else(|| anyhow::anyhow!("No destination in non-dry-run mode"))?;

    // 4. Insert into destination using the same method the engine uses
    let inserted = dest
        .insert_knowledge_collection(&kc_ual, &knowledge_assets, Some(&metadata), None)
        .await?;

    Ok(inserted)
}

/// Decode burned token IDs from the compact encoding stored in SQL.
///
/// Mirrors the logic in `src/application/state_metadata/encoding.rs` which is
/// `pub(crate)` in the main binary and cannot be imported here.
fn decode_burned_ids(mode: u32, payload: &[u8], start: u64, end: u64) -> Vec<u64> {
    match mode {
        // None
        0 => Vec::new(),
        // All tokens burned
        1 => {
            if end < start {
                Vec::new()
            } else {
                (start..=end).collect()
            }
        }
        // SparseIds: each burned ID stored as 8 bytes LE
        2 => {
            if !payload.len().is_multiple_of(8) {
                eprintln!(
                    "Warning: invalid SparseIds payload length {} (not multiple of 8)",
                    payload.len()
                );
                return Vec::new();
            }
            payload
                .chunks_exact(8)
                .map(|chunk| {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(chunk);
                    u64::from_le_bytes(b)
                })
                .collect()
        }
        // Bitmap: bit array with base = start
        3 => {
            if end < start {
                return Vec::new();
            }
            let bit_len = (end - start + 1) as usize;
            let expected_bytes = bit_len.div_ceil(8);
            if payload.len() != expected_bytes {
                eprintln!(
                    "Warning: invalid Bitmap payload length {} (expected {})",
                    payload.len(),
                    expected_bytes
                );
                return Vec::new();
            }
            let mut out = Vec::new();
            for i in 0..bit_len {
                let byte_idx = i / 8;
                let bit_idx = i % 8;
                if payload[byte_idx] & (1u8 << bit_idx) != 0 {
                    out.push(start + i as u64);
                }
            }
            out
        }
        other => {
            eprintln!(
                "Warning: unknown burned_mode {}, treating as no burns",
                other
            );
            Vec::new()
        }
    }
}

fn print_progress(
    processed: u64,
    total: u64,
    migrated: u64,
    skipped_existing: u64,
    skipped_empty: u64,
    failed: u64,
    start: &Instant,
) {
    let elapsed = start.elapsed().as_secs_f64();
    let rate = if elapsed > 0.0 {
        processed as f64 / elapsed
    } else {
        0.0
    };
    println!(
        "[{}/~{}] migrated={} skipped_exist={} skipped_empty={} failed={} | {:.1} KC/s",
        processed, total, migrated, skipped_existing, skipped_empty, failed, rate
    );
}
