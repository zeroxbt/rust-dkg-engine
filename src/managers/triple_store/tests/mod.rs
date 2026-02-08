mod integration_tests;
mod manager;

pub(super) fn require_blazegraph() -> bool {
    let backend = std::env::var("TRIPLE_STORE_TEST_BACKEND")
        .ok()
        .map(|value| value.to_ascii_lowercase());
    if backend.as_deref() == Some("blazegraph")
        || std::env::var("RUN_BLAZEGRAPH_TESTS").ok().as_deref() == Some("1")
    {
        true
    } else {
        eprintln!("Skipping Blazegraph tests (set RUN_BLAZEGRAPH_TESTS=1)");
        false
    }
}
