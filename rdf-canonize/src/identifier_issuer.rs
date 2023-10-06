use std::collections::HashMap;

#[derive(Clone)]
pub struct IdentifierIssuer {
    pub prefix: String,
    pub existing: HashMap<String, String>,
    pub counter: usize,
}

impl IdentifierIssuer {
    pub fn new(prefix: String, existing: Option<HashMap<String, String>>, counter: usize) -> Self {
        IdentifierIssuer {
            prefix,
            existing: existing.unwrap_or_default(),
            counter,
        }
    }

    pub fn get_id(&mut self, old: Option<&str>) -> String {
        if let Some(old) = old {
            if let Some(existing) = self.existing.get(old) {
                return existing.clone();
            }
        }

        let identifier = format!("{}{}", self.prefix, self.counter);
        self.counter += 1;

        if let Some(old) = old {
            self.existing.insert(old.to_string(), identifier.clone());
        }

        identifier
    }

    pub fn has_id(&self, old: &str) -> bool {
        self.existing.contains_key(old)
    }

    pub fn get_old_ids(&self) -> Vec<String> {
        self.existing.keys().cloned().collect()
    }
}
