use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::{collections::hash_map::DefaultHasher, hash::Hasher};
use crate::storage;

pub trait Storeable {}

pub trait StoreableItem<'a> {
    type Hasher: Hasher;
    type Error;

    /// Create a Bytes from this StoreableItem
    fn to_vec(&self) -> Result<Vec<u8>, Self::Error>;

    /// Create a StoreableItem from a Bytes
    fn from_vec(bytes: &'a Bytes) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Get the key bytes for this StoreableItem
    fn hashkey(&self) -> Result<Vec<u8>, Self::Error>;
}

impl<'a, T: Serialize + Deserialize<'a> + Storeable> StoreableItem<'a> for T {
    type Hasher = DefaultHasher;
    type Error = storage::Error;

    fn to_vec(&self) -> Result<Vec<u8>, Self::Error> {
        match bincode::serialize(&self) {
            Ok(bs) => Ok(bs),
            Err(e) => Err(e.into()),
        }
    }

    fn from_vec(bytes: &'a Bytes) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let this = match bincode::deserialize(bytes) {
            Ok(this) => this,
            Err(e) => return Err(e.into()),
        };
        Ok(this)
    }

    fn hashkey(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(bincode::serialize(&format!("{}", self.try_hash()?))?)
    }
}

pub trait TryHash<'a> {
    type Error;
    type Hasher: Hasher;

    fn try_hash(&self) -> Result<u64, Self::Error>;
}

impl<'a, T: Serialize + Deserialize<'a> + Storeable> TryHash<'a> for T {
    type Error = storage::Error;
    type Hasher = DefaultHasher;

    fn try_hash(&self) -> Result<u64, Self::Error> {
        let mut hasher = DefaultHasher::new();
        let bytes = bincode::serialize(&self)?;
        bytes.hash(&mut hasher);
        Ok(hasher.finish())
    }
}
