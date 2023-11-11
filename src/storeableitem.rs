use std::{collections::hash_map::DefaultHasher, hash::Hasher};
use std::hash::Hash;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub trait StoreableItem<'a> {
    type Hasher: Hasher;
    type Error;

    /// Create a Bytes from this StoreableItem
    fn to_vec(&self) -> Result<Vec<u8>, Self::Error>;
    
    /// Create a StoreableItem from a Bytes
    fn from_vec(bytes: &'a Bytes) -> Result<Self, Self::Error> where Self: Sized;

    /// Get the key bytes for this StoreableItem
    fn key(&self) -> Result<Vec<u8>, Self::Error>;
}

impl<'a, T: Serialize + Deserialize<'a> + Hash> StoreableItem<'a> for T {
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

    fn key(&self) -> Result<Vec<u8>, Self::Error> {
        let mut hasher = Self::Hasher::new();
        self.hash(&mut hasher);
        match bincode::serialize(&hasher.finish()) {
            Ok(bs) => Ok(bs),
            Err(e) => Err(e.to_string()),
        }
    }
}