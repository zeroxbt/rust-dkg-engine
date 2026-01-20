use serde::Serialize;

#[derive(Serialize)]
pub struct InfoResponse {
    pub version: &'static str,
}

impl InfoResponse {
    pub fn new(version: &'static str) -> Self {
        Self { version }
    }
}
