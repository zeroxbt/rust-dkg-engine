use std::{
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

fn parse_literal_u64(term: &oxigraph::model::Term) -> Option<u64> {
    match term {
        oxigraph::model::Term::Literal(lit) => lit.value().parse::<u64>().ok(),
        _ => None,
    }
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    let resolved_store_path = resolve_store_path(&args.store_path);
    let store = open_store(
        &resolved_store_path,
        args.oxigraph_max_open_files,
        args.oxigraph_fd_reserve,
    )?;

    let limit_clause = args
        .limit
        .map(|limit| format!("LIMIT {limit}"))
        .unwrap_or_default();

    let query = format!(
        r#"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT ?kc (MIN(?token) AS ?start_token_id) (MAX(?token) AS ?end_token_id) WHERE {{
            GRAPH <{metadata_graph}> {{
                ?kc <{has_named_graph}> ?g .
                FILTER(STRENDS(STR(?g), "/public"))
                BIND(REPLACE(STR(?g), CONCAT("^", STR(?kc), "/"), "") AS ?tail)
                BIND(REPLACE(?tail, "/public$", "") AS ?token_str)
                FILTER(REGEX(?token_str, "^[0-9]+$"))
                BIND(xsd:integer(?token_str) AS ?token)
            }}
        }}
        GROUP BY ?kc
        ORDER BY ?kc
        {limit_clause}"#,
        metadata_graph = METADATA_GRAPH,
        has_named_graph = HAS_NAMED_GRAPH,
        limit_clause = limit_clause,
    );

    let prepared = SparqlEvaluator::new()
        .parse_query(&query)
        .map_err(|e| format!("failed to parse SPARQL query: {e}"))?;

    let results = prepared
        .on_store(&store)
        .execute()
        .map_err(|e| format!("failed to execute SPARQL query: {e}"))?;

    let mut items = Vec::<WorkItem>::new();

    match results {
        QueryResults::Solutions(solutions) => {
            let vars: Vec<oxigraph::model::Variable> = solutions.variables().to_vec();
            let g_var = vars
                .iter()
                .find(|v| v.as_str() == "kc")
                .cloned()
                .ok_or_else(|| "query results missing variable ?kc".to_string())?;
            let start_var = vars
                .iter()
                .find(|v| v.as_str() == "start_token_id")
                .cloned()
                .ok_or_else(|| "query results missing variable ?start_token_id".to_string())?;
            let end_var = vars
                .iter()
                .find(|v| v.as_str() == "end_token_id")
                .cloned()
                .ok_or_else(|| "query results missing variable ?end_token_id".to_string())?;

            for solution in solutions {
                let solution = solution.map_err(|e| format!("failed to read solution row: {e}"))?;
                let kc_term = match solution.get(&g_var) {
                    Some(term) => term,
                    None => continue,
                };
                let start_term = match solution.get(&start_var) {
                    Some(term) => term,
                    None => continue,
                };
                let end_term = match solution.get(&end_var) {
                    Some(term) => term,
                    None => continue,
                };

                let kc_ual = match extract_iri(kc_term) {
                    Some(value) => value,
                    None => continue,
                };
                let start_token_id = match parse_literal_u64(start_term) {
                    Some(value) => value,
                    None => continue,
                };
                let end_token_id = match parse_literal_u64(end_term) {
                    Some(value) => value,
                    None => continue,
                };

                items.push(WorkItem {
                    kc_ual,
                    start_token_id,
                    end_token_id,
                    burned: Vec::new(),
                });
            }
        }
        _ => return Err("expected SELECT solutions, got non-solution result".to_string()),
    }

    items.sort_by(|a, b| a.kc_ual.cmp(&b.kc_ual));

    let payload = serde_json::to_string_pretty(&items)
        .map_err(|e| format!("failed to serialize workload JSON: {e}"))?;
    fs::write(&args.output, payload)
        .map_err(|e| format!("failed to write {}: {e}", args.output.display()))?;

    eprintln!(
        "store={} wrote {} workload entries to {}",
        resolved_store_path.display(),
        items.len(),
        args.output.display()
    );

    Ok(())
}
