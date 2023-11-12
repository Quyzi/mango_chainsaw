use std::collections::HashMap;
use bytes::Bytes;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use serde_derive::{Serialize, Deserialize};
use crate::internal::*;

/// A key=value pair to be stored in a sled tree
#[derive(Clone, Serialize, Deserialize)]
pub struct SledEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum TreeItem {
    Blob(SledEntry),
    Label(SledEntry),
    Relation(SledEntry),
}

/// An Object to be stored
/// 
/// Including its namespace and labels
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Object {
    pub namespace: String,
    pub id: u64,
    pub blob: Bytes, 
    pub labels: HashMap<String, String>,
}

impl Object {
    pub fn new(db: &DB) -> Result<Self> {
        Ok(Self {
            id: db.next_id()?,
            ..Default::default()
        })
    }

    pub fn with_namespace(mut self, name: &str) -> Self {
        self.namespace = name.to_string();
        self
    }

    pub fn with_blob(mut self, blob: Bytes) -> Self {
        self.blob = blob;
        self
    }

    pub fn with_labels(mut self, labels: Vec<Label>) -> Self {
        for label in labels {
            self.labels.insert(label.name, label.value);
        }
        self
    }

    fn get_blob_item(&self) -> Result<TreeItem> {
        let this = TreeItem::Blob(SledEntry { 
            key:   bincode::serialize(&format!("{}", &self.id))?, 
            value: self.blob.to_vec(),
        });
        Ok(this)
    }

    pub fn into_items(&self) -> Result<Vec<TreeItem>> {
        let mut items = vec![];
        items.push(self.get_blob_item()?);
        
        Ok(items)
    }
}