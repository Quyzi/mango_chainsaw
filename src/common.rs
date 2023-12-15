use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
};

use bytes::Bytes;
use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Label {
    pub data: String,
}

impl Label {
    pub fn new(s: &str) -> Self {
        Self {
            data: s.to_string(),
        }
    }

    pub fn id(&self) -> LabelID {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

pub type ObjectID = u64;
pub type Object = Arc<Bytes>;
pub type LabelID = u64;
