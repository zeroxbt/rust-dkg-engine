/// Visibility for triple store graph operations.
///
/// Unlike `Visibility` (application level, includes `All`), this enum
/// represents the actual named graph suffix used in SPARQL queries.
/// The triple store manager operates on individual graphs, so it only
/// accepts `Public` or `Private` - never both at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphVisibility {
    /// Public named graph (`{ual}/public`)
    Public,
    /// Private named graph (`{ual}/private`)
    Private,
}

impl GraphVisibility {
    /// Get the graph suffix string for SPARQL queries
    pub fn as_suffix(&self) -> &'static str {
        match self {
            GraphVisibility::Public => "public",
            GraphVisibility::Private => "private",
        }
    }
}
