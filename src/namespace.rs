use anyhow::{Result, anyhow};
use bytes::Bytes;
use flexbuffers::FlexbufferSerializer;
use serde::Serialize;
use sled::Tree;
use crate::common::*;

/// Separator character for tree names.
pub(crate) const SEPARATOR: &str = "\u{001F}";

/// A `Namespace` is a collection of `Object`s and `Label`s. 
/// 
/// The intention is for a `Namespace` to contain `Object`s that are loosely related. 
/// Each `Namespace` is separated from the others. 
/// 
/// Opening a `Namespace` by name will create or use existing data if present.f
#[derive(Clone, Debug)]
pub struct Namespace {
    /// Whats my name?
    pub name: String,

    /// Link back to parent Db
    #[allow(dead_code)]
    pub(crate) db: sled::Db,

    /// [Label ID] => [Label]
    pub(crate) labels: Tree,

    /// [Label content] => [Label ID]
    pub(crate) labels_inverse: Tree,

    /// [Object ID] => [Object Bytes]
    pub(crate) data: Tree,

    /// [Object ID] => [Vec<Label ID>]
    pub(crate) data_labels: Tree,

    /// [Label ID] => [Vec<Object ID>]
    pub(crate) data_labels_inverse: Tree,
}

impl Namespace {
    /// Open a `Namespace` by name from a Db
    pub(crate) fn open_from_db(db: sled::Db, name: &str) -> Result<Self> {
        Ok(Self {
            name: name.to_string(),
            db: db.clone(),
            labels: db.open_tree(format!("{name}{SEPARATOR}labels"))?,
            labels_inverse: db.open_tree(format!("{name}{SEPARATOR}labels_inverse"))?,
            data: db.open_tree(format!("{name}{SEPARATOR}data"))?,
            data_labels: db.open_tree(format!("{name}{SEPARATOR}data_labels"))?,
            data_labels_inverse: db.open_tree(format!("{name}{SEPARATOR}data_labels_inverse"))?,
        })
    }

    /// Serialization helper fn
    pub(crate) fn ser<T: Serialize>(thing: T) -> Result<Vec<u8>> {
        let mut s = FlexbufferSerializer::new();
        thing.serialize(&mut s)?;
        Ok(s.take_buffer())
    }

    /// Get an `Object` from this `Namespace` by its ID. 
    pub fn get(&self, id: ObjectID) -> Result<Option<Bytes>> {
        let kb = Self::ser(id)?;
        match self.data.get(kb) {
            Ok(Some(bs)) => Ok(Some(Bytes::from(bs.to_vec()))),
            Ok(None) => Ok(None),
            Err(e) => Err(anyhow!(e)),
        }
    }
}
