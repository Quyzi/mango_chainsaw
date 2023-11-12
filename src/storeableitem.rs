use std::{collections::hash_map::DefaultHasher, hash::Hasher};
use std::hash::Hash;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub trait Storeable {}

pub trait StoreableItem<'a> {
    type Hasher: Hasher;
    type Error;

    /// Create a Bytes from this StoreableItem
    fn to_vec(&self) -> Result<Vec<u8>, Self::Error>;
    
    /// Create a StoreableItem from a Bytes
    fn from_vec(bytes: &'a Bytes) -> Result<Self, Self::Error> where Self: Sized;

    /// Get the key bytes for this StoreableItem
    fn hashkey(&self) -> Result<Vec<u8>, Self::Error>;
}

impl<'a, T: Serialize + Deserialize<'a> + Storeable> StoreableItem<'a> for T {
    type Hasher = DefaultHasher;
    type Error = String;

    fn to_vec(&self) -> Result<Vec<u8>, Self::Error> {
        match bincode::serialize(&self) {
            Ok(bs) => Ok(bs),
            Err(e) => Err(e.to_string()),
        }
    }

    fn from_vec(bytes: &'a Bytes) -> Result<Self, Self::Error> where Self: Sized {
        let this = match bincode::deserialize(&bytes) {
            Ok(this) => this,
            Err(e) => return Err(e.to_string()),
        };
        Ok(this)
    }

    fn hashkey(&self) -> Result<Vec<u8>, Self::Error> {
        bincode::serialize(&format!("{}", self.try_hash()?)).map_err(|e| format!("{e}"))
    }    
}

pub trait TryHash<'a> {
    type Error;
    type Hasher: Hasher;

    fn try_hash(&self) -> Result<u64, Self::Error>;
}

impl<'a, T: Serialize + Deserialize<'a> + Storeable> TryHash<'a> for T {
    type Error = String;
    type Hasher = DefaultHasher;

    fn try_hash(&self) -> Result<u64, Self::Error> {
        let mut hasher = DefaultHasher::new();
        let bytes = bincode::serialize(&self).map_err(|e| format!("{e}"))?;
        bytes.hash(&mut hasher);
        Ok(hasher.finish())
    }
}