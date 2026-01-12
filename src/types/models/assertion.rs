use serde::{Deserialize, Serialize};
use validator_derive::Validate;

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
pub struct Assertion {
    #[validate(length(min = 1))]
    pub public: Vec<String>,
    pub private: Option<Vec<String>>,
}
