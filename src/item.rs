use bytes::Bytes;
use flexbuffers::FlexbufferSerializer;
use serde::de::DeserializeOwned;
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
    fn from_bytes(bytes: &'a Bytes) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Get the key bytes for this StoreableItem
    fn hashkey(&self) -> Result<Vec<u8>, Self::Error>;
}

impl<'a, T: Serialize + Storeable + Deserialize<'a>> StoreableItem<'a> for T {
    type Hasher = DefaultHasher;
    type Error = storage::Error;

    fn to_vec(&self) -> Result<Vec<u8>, Self::Error> {
        let mut s = FlexbufferSerializer::new();
        self.serialize(&mut s)?;
        Ok(s.take_buffer())
    }

    fn from_bytes(bytes: &'a Bytes) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(flexbuffers::from_slice(&bytes)? )
    }

    fn hashkey(&self) -> Result<Vec<u8>, Self::Error> {
        let mut s = FlexbufferSerializer::new();
        self.try_hash()?.serialize(&mut s)?;
        Ok(s.take_buffer())
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
        let mut s = FlexbufferSerializer::new();
        self.serialize(&mut s)?;
        let bytes: Vec<u8> = s.take_buffer();
        bytes.hash(&mut hasher);
        Ok(hasher.finish())
    }
}
