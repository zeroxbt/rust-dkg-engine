use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use clap::Parser;
use oxigraph::{
    sparql::{QueryResults, SparqlEvaluator},
    store::{Store, StoreOptions},
};
use rand::{SeedableRng, rngs::StdRng, seq::index::sample};
use serde::{Deserialize, Serialize};

const METADATA_GRAPH: &str = "metadata:graph";
const PUBLISHED_AT_BLOCK: &str = "https://ontology.origintrail.io/dkg/1.0#publishedAtBlock";
const HAS_KNOWLEDGE_ASSET: &str = "https://ontology.origintrail.io/dkg/1.0#hasKnowledgeAsset";
const DKG_REPOSITORY: &str = "DKG";

#[derive(Debug, Parser)]
#[command(name = "generate-workload")]
#[command(about = "Generate query-probe workload JSON from Oxigraph metadata graph")]
struct Args {
    /// Path to triple-store data root (the directory that contains DKG/)
    #[arg(long)]
    store_path: PathBuf,

    /// Output path for workload JSON file
    #[arg(long)]
    output: PathBuf,

    /// Optional maximum number of KCs to export
    #[arg(long)]
    limit: Option<usize>,

    /// Print progress every N processed candidates (0 disables periodic progress).
    #[arg(long, default_value_t = 100)]
    progress_every: usize,

    /// Number of KC UALs per boundary query batch.
    #[arg(long, default_value_t = 100)]
    boundary_batch_size: usize,

    /// Generate candidates from a numeric KC id range instead of scanning metadata.
    /// Example with other required flags:
    /// --blockchain-id otp:2043 --contract-address 0x... --max-kc-id 4333366 --sample-size 20000
    #[arg(long)]
    blockchain_id: Option<String>,

    /// Contract address used for KC UAL synthesis in range mode.
    #[arg(long)]
    contract_address: Option<String>,

    /// Maximum KC id (inclusive) for range mode.
    #[arg(long)]
    max_kc_id: Option<u64>,

    /// Number of random KC ids to sample in range mode.
    #[arg(long)]
    sample_size: Option<usize>,

    /// RNG seed for reproducible range sampling.
    #[arg(long)]
    seed: Option<u64>,

    /// Optional max_open_files for Oxigraph open options
    #[arg(long)]
    oxigraph_max_open_files: Option<u32>,

    /// Optional fd_reserve for Oxigraph open options
    #[arg(long)]
    oxigraph_fd_reserve: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkItem {
    kc_ual: String,
    start_token_id: u64,
    end_token_id: u64,
    burned: Vec<u64>,
}

fn open_store(
    path: &Path,
    max_open_files: Option<u32>,
    fd_reserve: Option<u32>,
) -> Result<Store, String> {
    let mut options = StoreOptions::default();
    if let Some(max_open_files) = max_open_files {
        options = options.with_max_open_files(max_open_files);
    }
    if let Some(fd_reserve) = fd_reserve {
        options = options.with_fd_reserve(fd_reserve);
    }

    Store::open_with_options(path, options)
        .map_err(|e| format!("failed to open Oxigraph store at {}: {e}", path.display()))
}

fn resolve_store_path(path: &Path) -> PathBuf {
    if path.file_name().is_some_and(|name| name == DKG_REPOSITORY) {
        return path.to_path_buf();
    }
    path.join(DKG_REPOSITORY)
}

fn extract_iri(term: &oxigraph::model::Term) -> Option<String> {
    if !term.is_named_node() {
        return None;
    }
    let raw = term.to_string();
    Some(
        raw.trim_start_matches('<')
            .trim_end_matches('>')
            .to_string(),
    )
}

fn query_candidate_kcs(store: &Store, limit: Option<usize>) -> Result<Vec<String>, String> {
    let limit_clause = limit
        .map(|value| format!("LIMIT {value}"))
        .unwrap_or_default();

    let primary_query = format!(
        r#"SELECT ?kc WHERE {{
            GRAPH <{metadata_graph}> {{
                ?kc <{published_at_block}> ?block .
            }}
        }}
        ORDER BY ?kc
        {limit_clause}"#,
        metadata_graph = METADATA_GRAPH,
        published_at_block = PUBLISHED_AT_BLOCK,
        limit_clause = limit_clause,
    );

    let mut candidates = Vec::new();
    {
        let prepared = SparqlEvaluator::new()
            .parse_query(&primary_query)
            .map_err(|e| format!("failed to parse primary SPARQL query: {e}"))?;
        let results = prepared
            .on_store(store)
            .execute()
            .map_err(|e| format!("failed to execute primary SPARQL query: {e}"))?;

        match results {
            QueryResults::Solutions(solutions) => {
                let vars: Vec<oxigraph::model::Variable> = solutions.variables().to_vec();
                let kc_var = vars
                    .iter()
                    .find(|v| v.as_str() == "kc")
                    .cloned()
                    .ok_or_else(|| "primary query results missing variable ?kc".to_string())?;

                for solution in solutions {
                    let solution =
                        solution.map_err(|e| format!("failed to read primary row: {e}"))?;
                    let Some(term) = solution.get(&kc_var) else {
                        continue;
                    };
                    if let Some(kc_ual) = extract_iri(term) {
                        candidates.push(kc_ual);
                    }
                }
            }
            _ => {
                return Err(
                    "primary query expected SELECT solutions, got non-solution result".to_string(),
                );
            }
        }
    }

    if !candidates.is_empty() {
        return Ok(candidates);
    }

    let fallback_query = format!(
        r#"SELECT DISTINCT ?kc WHERE {{
            GRAPH <{metadata_graph}> {{
                ?kc <{has_knowledge_asset}> ?ka .
            }}
        }}
        ORDER BY ?kc
        {limit_clause}"#,
        metadata_graph = METADATA_GRAPH,
        has_knowledge_asset = HAS_KNOWLEDGE_ASSET,
        limit_clause = limit_clause,
    );

    let prepared = SparqlEvaluator::new()
        .parse_query(&fallback_query)
        .map_err(|e| format!("failed to parse fallback SPARQL query: {e}"))?;
    let results = prepared
        .on_store(store)
        .execute()
        .map_err(|e| format!("failed to execute fallback SPARQL query: {e}"))?;
    match results {
        QueryResults::Solutions(solutions) => {
            let vars: Vec<oxigraph::model::Variable> = solutions.variables().to_vec();
            let kc_var = vars
                .iter()
                .find(|v| v.as_str() == "kc")
                .cloned()
                .ok_or_else(|| "fallback query results missing variable ?kc".to_string())?;
            for solution in solutions {
                let solution = solution.map_err(|e| format!("failed to read fallback row: {e}"))?;
                let Some(term) = solution.get(&kc_var) else {
                    continue;
                };
                if let Some(kc_ual) = extract_iri(term) {
                    candidates.push(kc_ual);
                }
            }
        }
        _ => {
            return Err(
                "fallback query expected SELECT solutions, got non-solution result".to_string(),
            );
        }
    }

    Ok(candidates)
}

fn query_candidate_kcs_from_range(args: &Args) -> Result<Vec<String>, String> {
    let blockchain_id = args
        .blockchain_id
        .as_deref()
        .ok_or_else(|| "range mode requires --blockchain-id".to_string())?;
    let contract_address = args
        .contract_address
        .as_deref()
        .ok_or_else(|| "range mode requires --contract-address".to_string())?;
    let max_kc_id = args
        .max_kc_id
        .ok_or_else(|| "range mode requires --max-kc-id".to_string())?;
    let sample_size = args
        .sample_size
        .ok_or_else(|| "range mode requires --sample-size".to_string())?;

    if max_kc_id == 0 {
        return Err("--max-kc-id must be > 0".to_string());
    }
    if sample_size == 0 {
        return Ok(Vec::new());
    }

    let population = max_kc_id as usize;
    let effective_sample = sample_size.min(population);
    let seed = args.seed.unwrap_or(0xD1CE_BA5E_u64);
    let mut rng = StdRng::seed_from_u64(seed);
    let indices = sample(&mut rng, population, effective_sample);

    let mut candidates = Vec::with_capacity(effective_sample);
    for idx in indices.into_iter() {
        let id = (idx as u64) + 1;
        candidates.push(format!(
            "did:dkg:{}/{}/{}",
            blockchain_id, contract_address, id
        ));
    }

    Ok(candidates)
}

fn query_kc_boundaries_batch(
    store: &Store,
    kc_uals: &[String],
) -> Result<HashMap<String, (u64, u64)>, String> {
    if kc_uals.is_empty() {
        return Ok(HashMap::new());
    }

    let values = kc_uals
        .iter()
        .map(|kc| format!("<{kc}>"))
        .collect::<Vec<_>>()
        .join(" ");

    let query = format!(
        r#"SELECT ?kc ?ka WHERE {{
            GRAPH <{metadata_graph}> {{
                VALUES ?kc {{ {values} }}
                ?kc <{has_knowledge_asset}> ?ka .
            }}
        }}"#,
        metadata_graph = METADATA_GRAPH,
        values = values,
        has_knowledge_asset = HAS_KNOWLEDGE_ASSET,
    );

    let prepared = SparqlEvaluator::new()
        .parse_query(&query)
        .map_err(|e| format!("failed to parse batch boundary query: {e}"))?;
    let results = prepared
        .on_store(store)
        .execute()
        .map_err(|e| format!("failed to execute batch boundary query: {e}"))?;

    let mut boundaries: HashMap<String, (u64, u64)> = HashMap::new();
    match results {
        QueryResults::Solutions(solutions) => {
            let vars: Vec<oxigraph::model::Variable> = solutions.variables().to_vec();
            let kc_var = vars
                .iter()
                .find(|v| v.as_str() == "kc")
                .cloned()
                .ok_or_else(|| "batch boundary query results missing variable ?kc".to_string())?;
            let ka_var = vars
                .iter()
                .find(|v| v.as_str() == "ka")
                .cloned()
                .ok_or_else(|| "batch boundary query results missing variable ?ka".to_string())?;

            for solution in solutions {
                let solution =
                    solution.map_err(|e| format!("failed to read batch boundary row: {e}"))?;
                let Some(kc_term) = solution.get(&kc_var) else {
                    continue;
                };
                let Some(ka_term) = solution.get(&ka_var) else {
                    continue;
                };

                let Some(kc_ual) = extract_iri(kc_term) else {
                    continue;
                };
                let Some(ka_ual) = extract_iri(ka_term) else {
                    continue;
                };

                let Some((prefix, token_str)) = ka_ual.rsplit_once('/') else {
                    continue;
                };
                if prefix != kc_ual {
                    continue;
                }
                let Ok(token_id) = token_str.parse::<u64>() else {
                    continue;
                };

                boundaries
                    .entry(kc_ual)
                    .and_modify(|(start, end)| {
                        *start = (*start).min(token_id);
                        *end = (*end).max(token_id);
                    })
                    .or_insert((token_id, token_id));
            }
        }
        _ => return Err("batch boundary query expected SELECT solutions".to_string()),
    }

    Ok(boundaries)
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    let resolved_store_path = resolve_store_path(&args.store_path);
    let store = open_store(
        &resolved_store_path,
        args.oxigraph_max_open_files,
        args.oxigraph_fd_reserve,
    )?;

    let range_mode = args.blockchain_id.is_some()
        || args.contract_address.is_some()
        || args.max_kc_id.is_some()
        || args.sample_size.is_some();
    let candidates = if range_mode {
        query_candidate_kcs_from_range(&args)?
    } else {
        query_candidate_kcs(&store, args.limit)?
    };
    let candidate_count = candidates.len();
    let mut items = Vec::<WorkItem>::with_capacity(candidate_count);
    let started = Instant::now();

    if candidate_count == 0 {
        eprintln!(
            "store={} mode={} candidates=0 (nothing to process)",
            resolved_store_path.display(),
            if range_mode { "range" } else { "scan" },
        );
    } else {
        eprintln!(
            "store={} mode={} candidates={} progress_every={}",
            resolved_store_path.display(),
            if range_mode { "range" } else { "scan" },
            candidate_count,
            args.progress_every
        );
    }

    let batch_size = args.boundary_batch_size.max(1);
    let mut processed = 0usize;
    for chunk in candidates.chunks(batch_size) {
        let boundaries = query_kc_boundaries_batch(&store, chunk)?;
        for kc_ual in chunk {
            if let Some((start_token_id, end_token_id)) = boundaries.get(kc_ual) {
                items.push(WorkItem {
                    kc_ual: kc_ual.clone(),
                    start_token_id: *start_token_id,
                    end_token_id: *end_token_id,
                    burned: Vec::new(),
                });
            }
        }

        processed = processed.saturating_add(chunk.len());
        if args.progress_every > 0
            && (processed % args.progress_every == 0 || processed == candidate_count)
        {
            let elapsed = started.elapsed();
            let found = items.len();
            let misses = processed.saturating_sub(found);
            let rate = if elapsed.as_secs_f64() > 0.0 {
                processed as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            };
            let remaining = candidate_count.saturating_sub(processed);
            let eta = if rate > 0.0 {
                Duration::from_secs_f64(remaining as f64 / rate)
            } else {
                Duration::from_secs(0)
            };
            eprintln!(
                "progress processed={}/{} found={} misses={} hit_rate={:.2}% elapsed={} eta={} rate={:.2}/s",
                processed,
                candidate_count,
                found,
                misses,
                (found as f64 / processed as f64) * 100.0,
                format_duration(elapsed),
                format_duration(eta),
                rate
            );
        }
    }

    items.sort_by(|a, b| a.kc_ual.cmp(&b.kc_ual));

    let payload = serde_json::to_string_pretty(&items)
        .map_err(|e| format!("failed to serialize workload JSON: {e}"))?;
    fs::write(&args.output, payload)
        .map_err(|e| format!("failed to write {}: {e}", args.output.display()))?;

    eprintln!(
        "store={} mode={} candidates={} wrote {} workload entries to {}",
        resolved_store_path.display(),
        if range_mode { "range" } else { "scan" },
        candidate_count,
        items.len(),
        args.output.display()
    );

    Ok(())
}

fn format_duration(duration: Duration) -> String {
    let total = duration.as_secs();
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    format!("{h:02}:{m:02}:{s:02}")
}
