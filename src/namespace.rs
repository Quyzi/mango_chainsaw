use crate::common::*;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use flexbuffers::FlexbufferSerializer;
use minitrace::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use sled::Tree;

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
    #[trace]
    pub(crate) fn ser<T: Serialize>(thing: T) -> Result<Vec<u8>> {
        let mut s = FlexbufferSerializer::new();
        thing.serialize(&mut s)?;
        Ok(s.take_buffer())
    }

    /// Deserialization helper fn
    #[trace]
    pub(crate) fn de<T: DeserializeOwned>(thing: Vec<u8>) -> Result<T> {
        let this = flexbuffers::from_slice(&thing)?;
        Ok(this)
    }

    /// Get a single object by id
    #[trace]
    pub fn get_one(&self, id: ObjectID) -> Result<(ObjectID, Option<Vec<Label>>, Option<Bytes>)> {
        let kb = Self::ser(id)?;
        let bytes = self
            .data
            .get(kb.clone())?
            .map(|bytes| Bytes::from(bytes.to_vec()));

        if bytes.is_none() {
            return Ok((id, None, None));
        }

        let labelids: Vec<LabelID> = match self.data_labels.get(kb.clone()) {
            Ok(Some(bytes)) => Self::de(bytes.to_vec())?,
            Ok(None) => vec![],
            Err(e) => return Err(anyhow!("{e}")),
        };

        let mut labels = vec![];
        for id in labelids {
            let key = Self::ser(id)?;
            let label = match self.labels.get(key) {
                Ok(Some(bytes)) => {
                    let label: Label = Self::de(bytes.to_vec())?;
                    label
                }
                Ok(None) => continue,
                Err(e) => return Err(anyhow!("{e}")),
            };
            labels.push(label)
        }

        Ok((id, Some(labels), bytes))
    }

    /// Get all object ids
    #[allow(clippy::type_complexity)]
    #[trace]
    pub fn get_all(
        &self,
        ids: Vec<ObjectID>,
    ) -> Result<Vec<(ObjectID, Option<Vec<Label>>, Option<Bytes>)>> {
        let mut results = vec![];
        for id in ids {
            let this = self.get_one(id)?;
            results.push(this);
        }

        Ok(results)
    }
}
