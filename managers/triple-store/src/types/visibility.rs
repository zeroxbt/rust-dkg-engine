use serde::{Deserialize, Serialize};

/// Visibility level for knowledge triples
///
/// Controls which triples are retrieved or stored based on their
/// privacy annotations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Visibility {
    /// Only public triples (excludes those with "private" label)
    Public,
    /// Only private triples (those with "private" label)
    Private,
    /// All triples regardless of visibility
    All,
}

impl Visibility {
    /// Get the suffix used in named graph URIs
    ///
    /// Returns `Some("public")` or `Some("private")` for specific visibility,
    /// or `None` for `All` which doesn't map to a specific named graph.
    pub fn as_suffix(&self) -> Option<&'static str> {
        match self {
            Self::Public => Some("public"),
            Self::Private => Some("private"),
            Self::All => None,
        }
    }

    /// Check if this visibility should filter out private triples
    pub fn excludes_private(&self) -> bool {
        matches!(self, Self::Public)
    }
}

impl std::fmt::Display for Visibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Public => write!(f, "public"),
            Self::Private => write!(f, "private"),
            Self::All => write!(f, "all"),
        }
    }
}
