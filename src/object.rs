use std::{
    collections::hash_map::DefaultHasher,
    fmt::Display,
    hash::{Hash, Hasher},
};

use bytes::Bytes;
use serde::Serialize;
use serde_derive::{Deserialize, Serialize};
use sled::IVec;

pub type ObjectID = u64;

#[derive(Clone, Debug, Hash, Serialize, Deserialize)]
pub struct Object {
    inner: Bytes,
}

impl Object {
    pub fn new(bs: Bytes) -> Self {
        Self { inner: bs }
    }

    pub fn get_inner(&self) -> Bytes {
        self.inner.clone()
    }

    pub fn hash_id(&self) -> ObjectID {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<Bytes> for Object {
    fn from(value: Bytes) -> Self {
        Self { inner: value }
    }
}

impl TryFrom<IVec> for Object {
    type Error = anyhow::Error;

    fn try_from(value: IVec) -> Result<Self, Self::Error> {
        let this = flexbuffers::from_slice(&value)?;
        Ok(this)
    }
}

impl TryInto<IVec> for Object {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<IVec, Self::Error> {
        let mut s = flexbuffers::FlexbufferSerializer::new();
        self.serialize(&mut s)?;
        Ok(s.take_buffer().into())
    }
}

impl Display for Object {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self.inner)
    }
}
