use std::collections::HashSet;

use serde::Deserialize;

use crate::error::{Result, TripleStoreError};

#[derive(Deserialize)]
struct SparqlSelectResponse {
    results: SparqlSelectResults,
}

#[derive(Deserialize)]
struct SparqlSelectResults {
    bindings: Vec<std::collections::HashMap<String, SparqlSelectBinding>>,
}

#[derive(Deserialize)]
struct SparqlSelectBinding {
    value: String,
}

pub(crate) fn parse_select_values(json: &str, var: &str) -> Result<HashSet<String>> {
    let response: SparqlSelectResponse =
        serde_json::from_str(json).map_err(|e| TripleStoreError::ParseError {
            reason: format!("Failed to parse SELECT response: {e}"),
        })?;

    let mut values = HashSet::new();
    for binding in response.results.bindings {
        if let Some(value) = binding.get(var) {
            values.insert(value.value.clone());
        }
    }

    Ok(values)
}
