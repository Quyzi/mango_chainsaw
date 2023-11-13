use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::storage;

pub trait Metadata<'a> {
    type Error;
    type Item: MetadataItem<'a>;
    type ObjectId;

    fn new(id: Self::ObjectId, items: Vec<Self::Item>) -> Result<Self, Self::Error>
    where
        Self: Sized;

    fn items(&self) -> Vec<Self::Item>;
    fn id(&self) -> Self::ObjectId;
    fn db_key(&self) -> Result<Vec<u8>, Self::Error>;

    fn to_bytes(&self) -> Result<Vec<u8>, Self::Error>;
    fn from_bytes(bytes: &Bytes) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DefaultMetadata {
    objectid: u64,
    items: Vec<DefaultMetadataItem>,
}

impl<'a> Metadata<'a> for DefaultMetadata {
    type Error = storage::Error;
    type Item = DefaultMetadataItem;
    type ObjectId = u64;

    fn new(id: Self::ObjectId, items: Vec<Self::Item>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self {
            objectid: id,
            items,
        })
    }

    fn items(&self) -> Vec<Self::Item> {
        self.items.clone()
    }

    fn id(&self) -> Self::ObjectId {
        self.objectid
    }

    fn db_key(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(bincode::serialize(&self.id())?)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(bincode::serialize(&self)?)
    }

    fn from_bytes(bytes: &Bytes) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(bincode::deserialize(bytes)?)
    }
}

pub trait MetadataItem<'a> {
    type Error;
    type Key;
    type Value;

    fn new(k: Self::Key, v: Self::Value) -> Self;
    fn key(&self) -> Self::Key;
    fn value(&self) -> Self::Value;
    fn to_string(&self) -> String;
    fn key_bytes(&self) -> Result<Vec<u8>, Self::Error>;
    fn val_bytes(&self) -> Result<Vec<u8>, Self::Error>;
}

pub const DEFAULT_SEPARATOR: &str = "\u{001F}";

#[derive(Serialize, Deserialize, Clone)]
pub struct DefaultMetadataItem {
    pub key: String,
    pub value: String,
}

impl<'a> MetadataItem<'a> for DefaultMetadataItem {
    type Error = storage::Error;
    type Key = String;
    type Value = String;

    fn new(_k: Self::Key, _v: Self::Value) -> Self {
        todo!()
    }

    fn key(&self) -> Self::Key {
        self.key.clone()
    }

    fn value(&self) -> Self::Value {
        self.value.clone()
    }

    fn to_string(&self) -> String {
        format!("{}{DEFAULT_SEPARATOR}{}", self.key(), self.value())
    }

    fn key_bytes(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(bincode::serialize(&self.key().to_string())?)
    }

    fn val_bytes(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(bincode::serialize(&self.value().to_string())?)
    }
}
