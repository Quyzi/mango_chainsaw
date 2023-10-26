use serde_derive::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    pub db_path: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            db_path: "mangochainsaw.db".into(),
        }
    }
}
