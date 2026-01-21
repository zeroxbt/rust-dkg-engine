use std::collections::HashMap;

/// Manages identifiers for blank nodes in RDF datasets, ensuring unique identifiers are issued.
///
/// This struct is utilized in the canonicalization process to manage the issuance of new,
/// unique identifiers for blank nodes within an RDF graph.
#[derive(Clone)]
pub struct IdentifierIssuer {
    prefix: String,
    existing: HashMap<String, String>,
    counter: usize,
}

impl IdentifierIssuer {
    /// Returns the prefix used for new identifiers.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Constructs a new `IdentifierIssuer` with a specified prefix for identifiers.
    ///
    /// # Arguments
    /// * `prefix` - A string to prefix identifiers with, ensuring they are unique within the scope.
    /// * `existing` - An optional map from existing identifiers to new identifiers, allowing
    ///   continuation from previous state.
    /// * `counter` - The starting point for counting new identifiers.
    pub fn new(prefix: String, existing: Option<HashMap<String, String>>, counter: usize) -> Self {
        IdentifierIssuer {
            prefix,
            existing: existing.unwrap_or_default(),
            counter,
        }
    }

    /// Gets or creates a new identifier based on the old identifier if provided.
    ///
    /// # Arguments
    /// * `old` - An optional reference to an old identifier that might already have a new
    ///   identifier mapped.
    ///
    /// # Returns
    /// Returns a new identifier if one is created, or the existing identifier if one was already
    /// mapped.
    pub fn get_id(&mut self, old: Option<&str>) -> String {
        if let Some(old) = old
            && let Some(existing) = self.existing.get(old)
        {
            return existing.clone();
        }

        let identifier = format!("{}{}", self.prefix, self.counter);
        self.counter += 1;

        if let Some(old) = old {
            self.existing.insert(old.to_string(), identifier.clone());
        }

        identifier
    }

    /// Checks if an identifier exists for a given key.
    ///
    /// # Arguments
    /// * `old` - The key to check in the map of existing identifiers.
    ///
    /// # Returns
    /// Returns `true` if the identifier exists, otherwise `false`.
    pub fn has_id(&self, old: &str) -> bool {
        self.existing.contains_key(old)
    }

    /// Returns a list of all old identifiers currently mapped to new identifiers.
    pub fn get_old_ids(&self) -> Vec<String> {
        self.existing.keys().cloned().collect()
    }
}
