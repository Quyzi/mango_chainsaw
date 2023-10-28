
use serde_derive::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Label {
    pub(crate) name: String,
    pub(crate) value: String,
    // todo
    // pub(crate) weight: i8,
}

impl Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}={}", self.name, self.value)
    }
}

impl Label {
    pub fn new(name: &str, value: &str) -> Self {
        Self {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    pub(crate) fn key(&self) -> Vec<u8> {
        match bincode::serialize(&format!("{}={}", &self.name, &self.value)) {
            Ok(bytes) => bytes,
            Err(e) => {
                log::error!(target: "mango_chainsaw", "failed to serialize key for {self}: {e}");
                vec![]
            },
        }
    }
}
