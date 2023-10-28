use crate::internal::*;
use bytes::Bytes;
use rayon::prelude::*;
use sled::Tree;
use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::{Hash, Hasher},
};

/// `Namespace`  \
/// A pointer to a Namespace
#[derive(Debug)]
pub struct Namespace {
    /// Namespace name
    pub name: String,

    /// Blob storage
    blobs: Tree,

    /// Label storage
    labels: Tree,
}

impl Namespace {
    pub(crate) fn new(db: &DB, name: &str) -> Result<Self> {
        let blobs = db.inner.open_tree(format!("{name}_blobs"))?;
        let labels = db.inner.open_tree(format!("{name}_labels"))?;
        Ok(Self {
            name: name.to_string(),
            blobs,
            labels,
        })
    }

    pub(crate) fn drop(self, db: &DB) -> Result<()> {
        let name = &self.name;
        for tree in ["blobs", "labels"] {
            match db.inner.drop_tree(format!("{name}_{tree}")) {
                Ok(_) => log::debug!(target: "mango_chainsaw", "dropped tree {name}_{tree}"),
                Err(e) => {
                    log::error!(target: "mango_chainsaw", "failed to drop tree {name}_{tree} {e}");
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    /// Insert an object into this namespace \
    /// Use labels to index the object by key:value
    pub fn insert(&self, blob: Bytes, labels: Vec<Label>) -> Result<u64> {
        let hash = {
            let mut hasher = DefaultHasher::new();
            blob.hash(&mut hasher);
            hasher.finish()
        };
        match self.blobs.insert(bincode::serialize(&hash)?, blob.to_vec()) {
            Ok(_) => log::trace!(target: "mango_chainsaw", "inserted object with hash {hash}"),
            Err(e) => {
                log::error!(target: "mango_chainsaw", "failed to insert object with hash {hash}: {e}");
                return Err(e.into());
            }
        }

        for label in labels {
            match self.upsert_label(&label, hash) {
                Ok(_) => log::trace!(target: "mango_chainsaw", "upserted label {label}"),
                Err(e) => {
                    log::error!(target: "mango_chainsaw", "failed to upsert label {label}: {e}");
                    return Err(e);
                }
            }
        }

        Ok(hash)
    }

    pub(crate) fn upsert_label(&self, label: &Label, hash: u64) -> Result<()> {
        let upsert = |old: Option<&[u8]>| -> Option<Vec<u8>> {
            match old {
                Some(bytes) => {
                    let mut hashes: Vec<u64> = match bincode::deserialize::<Vec<u64>>(bytes) {
                        Ok(h) => {
                            log::trace!(target: "mango_chainsaw", "got {} hashes for label {label}", h.len());
                            h
                        }
                        Err(e) => {
                            log::error!(target: "mango_chainsaw", "failed to upsert label {label}: {e}");
                            vec![]
                        }
                    };
                    if !hashes.is_empty() {
                        hashes.push(hash);
                        if let Ok(bs) = bincode::serialize(&hashes) {
                            return Some(bs);
                        }
                        return None;
                    }
                    None
                }
                None => {
                    if let Ok(bs) = bincode::serialize(&vec![hash]) {
                        return Some(bs);
                    }
                    None
                }
            }
        };
        self.labels.update_and_fetch(label.key(), upsert)?;
        Ok(())
    }

    /// Get an object by ID
    pub fn get(&self, id: u64) -> Result<Option<Bytes>> {
        match self.blobs.get(bincode::serialize(&id)?) {
            Ok(Some(blob)) => Ok(Some(Bytes::from(blob.to_vec()))),
            Ok(None) => Ok(None),
            Err(e) => {
                log::error!(target: "mango_chainsaw", "error getting id={id}: {e}");
                return Err(e.into())
            },
        }
    }

    /// Get all objects matching the given labels
    pub fn query(&self, labels: Vec<Label>) -> Result<Vec<u64>> {
        let mut sets: Vec<HashSet<u64>> = labels.par_iter().map(|label| {
            let labels = self.labels.clone();
            match labels.get(label.key()) {
                Ok(Some(bytes)) => {
                    let hashes: Vec<u64> = match bincode::deserialize::<Vec<u64>>(&bytes) {
                        Ok(h) => {
                            log::debug!(target: "mango_chainsaw", "found {} matches for {label}", h.len());
                            h
                        },
                        Err(e) => {
                            log::error!(target: "mango_chainsaw", "failed to deserialize bytes for label {label}: {e}");
                            vec![]
                        },
                    };
                    hashes
                },
                Ok(None) => {
                    log::debug!(target: "mango_chainsaw", "found no matches for {label}");
                    vec![]
                },
                Err(e) => {
                    log::error!(target: "mango_chainsaw", "failed to get {label}: {e}");
                    vec![]
                },
            }.into_par_iter().collect::<HashSet<u64>>()
        }).collect();
        sets.sort_by_key(|a| a.len());

        let (intersection, others) = sets.split_at_mut(1);
        let intersection = &mut intersection[0];
        for other in others {
            intersection.retain(|e| other.contains(e))
        }
        Ok(Vec::from_iter(&mut intersection.to_owned().into_iter()))
    }

    /// Delete objects with the given ids. 
    pub fn delete_objects(&self, ids: Vec<u64>) -> Result<()> {
        for id in ids {
            match self.blobs.remove(bincode::serialize(&id)?) {
                Ok(_) => {
                    log::debug!(target: "mango_chainsaw", "removed blob {id}");
                },
                Err(e) => {
                    log::error!(target: "mango_chainsaw", "failed to remove blob {id}: {e}");
                    return Err(e.into())
                },
            }
        }
        Ok(())
    }

    /// WIP
    pub fn prune(&self) -> Result<()> {
        log::info!(target: "mango_chainsaw", "starting prune on namespace {}", self.name);
        let ids: Vec<u64> = self.blobs.into_iter().keys().map(|key| match key {
            Ok(k) => match bincode::deserialize::<u64>(&k) {
                Ok(id) => id,
                Err(e) => {
                    log::error!(target: "mango_chainsaw", "error deserializing key during prune {e}");
                    0
                }
            },
            Err(e) => {
                log::error!(target: "mango_chainsaw", "error pruning {e}");
                0
            }
        }).filter(|id| id != &0).collect();
        log::info!(target: "mango_chainsaw", "found {} blobs stored in namespace {}", ids.len(), self.name);

        Ok(())
    }
}
