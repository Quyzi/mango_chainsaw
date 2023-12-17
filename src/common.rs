use std::{
    collections::hash_map::DefaultHasher,
    fmt::Display,
    hash::{Hash, Hasher},
    sync::Arc,
};

use bytes::Bytes;
use serde_derive::{Deserialize, Serialize};

/// A Label is metadata describing an Object
#[derive(Clone, Debug, Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Label {
    pub data: String,
}

impl Label {
    /// Create a new label
    pub fn new(s: &str) -> Self {
        Self {
            data: s.to_string(),
        }
    }

    /// Get the id of the label \
    /// The id is the hash of the label contents
    pub fn id(&self) -> LabelID {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

/// ObjectID is the ID of an Object
pub type ObjectID = u64;

/// An Object is anything that can be serialized into a Bytes
pub type Object = Arc<Bytes>;

/// LabelID is the ID of a Label
pub type LabelID = u64;
