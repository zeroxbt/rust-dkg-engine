use serde::{Deserialize, Serialize};

/// Visibility level for knowledge triples.
///
/// Controls which triples are retrieved or stored based on their
/// privacy annotations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Visibility {
    /// Only public triples (excludes those with "private" label)
    Public,
    /// Only private triples (those with "private" label)
    Private,
    /// All triples regardless of visibility
    #[default]
    All,
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
