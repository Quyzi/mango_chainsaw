use anyhow::anyhow;
use bytes::Bytes;
use serde::Serialize;
use serde_derive::{Deserialize, Serialize};
use sled::IVec;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

pub const SEPARATOR: &str = "\u{001F}";

/// Labels are key=value pairs describing an Object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Label(pub(crate) String, pub(crate) String);

impl Label {
    /// Create a new label
    pub fn new(lhs: &str, rhs: &str) -> Self {
        Self(lhs.to_string(), rhs.to_string())
    }

    pub fn hash_id(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    pub fn to_string_ltr(&self) -> String {
        format!("{}{SEPARATOR}{}", self.0, self.1)
    }

    pub fn to_string_rtl(&self) -> String {
        format!("{}{SEPARATOR}{}", self.1, self.0)
    }
}

impl Hash for Label {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
        self.1.hash(state);
    }
}

impl TryFrom<String> for Label {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let mut s = value.split(SEPARATOR);
        let (lhs, rhs) = match (s.next(), s.next()) {
            (Some(l), Some(r)) => (l, r),
            _ => return Err(anyhow!("invalid label string")),
        };
        Ok(Self(lhs.to_string(), rhs.to_string()))
    }
}

impl TryFrom<IVec> for Label {
    type Error = anyhow::Error;

    fn try_from(value: IVec) -> Result<Self, Self::Error> {
        let this = flexbuffers::from_slice(&value)?;
        Ok(this)
    }
}

impl TryInto<IVec> for Label {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<IVec, Self::Error> {
        let mut s = flexbuffers::FlexbufferSerializer::new();
        self.serialize(&mut s)?;
        Ok(s.take_buffer().into())
    }
}

impl TryFrom<Bytes> for Label {
    type Error = anyhow::Error;

    fn try_from(value: Bytes) -> std::prelude::v1::Result<Self, Self::Error> {
        let this = flexbuffers::from_slice(&value)?;
        Ok(this)
    }
}

impl TryInto<Bytes> for Label {
    type Error = anyhow::Error;

    fn try_into(self) -> std::prelude::v1::Result<Bytes, Self::Error> {
        let mut s = flexbuffers::FlexbufferSerializer::new();
        self.serialize(&mut s)?;
        Ok(Bytes::from(s.take_buffer()))
    }
}
