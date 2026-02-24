use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use clap::Parser;
use oxigraph::{
    sparql::{QueryResults, SparqlEvaluator},
    store::{Store, StoreOptions},
};
use serde::{Deserialize, Serialize};

const METADATA_GRAPH: &str = "metadata:graph";
const HAS_NAMED_GRAPH: &str = "https://ontology.origintrail.io/dkg/1.0#hasNamedGraph";

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

fn parse_graph_into_kc_and_token(graph_iri: &str) -> Option<(String, u64)> {
    let without_public = graph_iri.strip_suffix("/public")?;
    let (kc_ual, token_str) = without_public.rsplit_once('/')?;
    let token_id = token_str.parse::<u64>().ok()?;
    Some((kc_ual.to_string(), token_id))
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    let store = open_store(
        &args.store_path,
        args.oxigraph_max_open_files,
        args.oxigraph_fd_reserve,
    )?;

    let query = format!(
        r#"SELECT ?g WHERE {{
            GRAPH <{metadata_graph}> {{
                ?kc <{has_named_graph}> ?g .
            }}
            FILTER(STRENDS(STR(?g), "/public"))
        }}"#,
        metadata_graph = METADATA_GRAPH,
        has_named_graph = HAS_NAMED_GRAPH,
    );

    let prepared = SparqlEvaluator::new()
        .parse_query(&query)
        .map_err(|e| format!("failed to parse SPARQL query: {e}"))?;

    let results = prepared
        .on_store(&store)
        .execute()
        .map_err(|e| format!("failed to execute SPARQL query: {e}"))?;

    let mut boundaries: HashMap<String, (u64, u64)> = HashMap::new();

    match results {
        QueryResults::Solutions(solutions) => {
            let vars: Vec<oxigraph::model::Variable> = solutions.variables().to_vec();
            let g_var = vars
                .iter()
                .find(|v| v.as_str() == "g")
                .cloned()
                .ok_or_else(|| "query results missing variable ?g".to_string())?;

            for solution in solutions {
                let solution = solution.map_err(|e| format!("failed to read solution row: {e}"))?;
                let term = match solution.get(&g_var) {
                    Some(term) => term,
                    None => continue,
                };

                let graph_iri = match extract_iri(term) {
                    Some(value) => value,
                    None => continue,
                };

                let (kc_ual, token_id) = match parse_graph_into_kc_and_token(&graph_iri) {
                    Some(value) => value,
                    None => continue,
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
        _ => return Err("expected SELECT solutions, got non-solution result".to_string()),
    }

    let mut items: Vec<WorkItem> = boundaries
        .into_iter()
        .map(|(kc_ual, (start_token_id, end_token_id))| WorkItem {
            kc_ual,
            start_token_id,
            end_token_id,
            burned: Vec::new(),
        })
        .collect();

    items.sort_by(|a, b| a.kc_ual.cmp(&b.kc_ual));
    if let Some(limit) = args.limit {
        items.truncate(limit);
    }

    let payload = serde_json::to_string_pretty(&items)
        .map_err(|e| format!("failed to serialize workload JSON: {e}"))?;
    fs::write(&args.output, payload)
        .map_err(|e| format!("failed to write {}: {e}", args.output.display()))?;

    eprintln!(
        "wrote {} workload entries to {}",
        items.len(),
        args.output.display()
    );

    Ok(())
}
