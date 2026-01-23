use serde::Serialize;

#[derive(Serialize)]
pub(crate) struct InfoResponse {
    pub version: &'static str,
}

impl InfoResponse {
    pub(crate) fn new(version: &'static str) -> Self {
        Self { version }
    }
}
