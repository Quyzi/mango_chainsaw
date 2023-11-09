use crate::internal::*;
use bytes::Bytes;
use rayon::prelude::*;
use serde_derive::{Deserialize, Serialize};
use sled::Tree;
use std::collections::HashSet;
use utoipa::{ToResponse, ToSchema};

/// `Namespace`  \
/// A pointer to a Namespace
#[derive(Debug)]
pub struct Namespace {
    /// Namespace name
    pub name: String,

    /// Blob storage
    blobs: Tree,

    /// Relationships between blobs and their labels
    blobs_labels: Tree,

    /// Label storage
    labels: Tree,

    /// Link back to the Db
    db: sled::Db,
}

impl Namespace {
    pub(crate) fn new(db: &DB, name: &str) -> Result<Self> {
        let blobs = db
            .inner
            .open_tree(bincode::serialize(&format!("{name}{SEPARATOR}blobs"))?)?;
        let labels = db
            .inner
            .open_tree(bincode::serialize(&format!("{name}{SEPARATOR}labels"))?)?;
        let relations = db
            .inner
            .open_tree(bincode::serialize(&format!("{name}{SEPARATOR}relations"))?)?;

        Ok(Self {
            name: name.to_string(),
            blobs,
            blobs_labels: relations,
            labels,
            db: db.inner.clone(),
        })
    }

    pub(crate) fn drop(self, db: &DB) -> Result<()> {
        let name = &self.name;
        for tree in ["blobs", "labels", "relations"] {
            match db
                .inner
                .drop_tree(bincode::serialize(&format!("{name}{SEPARATOR}{tree}"))?)
            {
                Ok(r) => {
                    log::debug!(target: "mango_chainsaw", "[{}] dropped tree {name}_{tree}, result: {r}", self.name)
                }
                Err(e) => {
                    log::error!(target: "mango_chainsaw", "[{}] failed to drop tree {name}_{tree} {e}", self.name);
                    return Err(e.into());
                }
            }
        }
        db.inner.flush()?;
        Ok(())
    }

    /// Insert an object into this namespace \
    /// Use labels to index the object by key:value
    pub fn insert(&self, blob: Bytes, labels: Vec<Label>) -> Result<u64> {
        let id = self.db.generate_id()?;
        match self
            .blobs
            .insert(bincode::serialize(&format!("{id}"))?, blob.to_vec())
        {
            Ok(_) => {
                log::trace!(target: "mango_chainsaw", "[{}] inserted object with id {id}", self.name)
            }
            Err(e) => {
                log::error!(target: "mango_chainsaw", "[{}] failed to insert object with id {id}: {e}", self.name);
                return Err(e.into());
            }
        }

        for label in &labels.to_owned() {
            match self.upsert_label(label, id) {
                Ok(_) => {
                    log::trace!(target: "mango_chainsaw", "[{}] upserted label {label}", self.name)
                }
                Err(e) => {
                    log::error!(target: "mango_chainsaw", "[{}] failed to upsert label {label}: {e}", self.name);
                    return Err(e);
                }
            }
        }
        self.insert_blobs_labels(id, labels)?;

        Ok(id)
    }

    /// Insert blob -> Labels relation into the Database
    ///
    /// It is used to clean up after deleting a blob.
    pub(crate) fn insert_blobs_labels(&self, id: u64, labels: Vec<Label>) -> Result<()> {
        let key = match bincode::serialize(&format!("{id}")) {
            Ok(bytes) => bytes,
            Err(e) => {
                log::error!(target: "mango_chainsaw", "[{}] failed to serialize key for id {id}: {e}", self.name);
                return Err(e.into());
            }
        };
        let labels: Vec<String> = labels.into_iter().map(|lbl| format!("{lbl}")).collect();
        let labelsbytes = match bincode::serialize(&labels) {
            Ok(bytes) => bytes,
            Err(e) => {
                log::error!(target: "mango_chainsaw", "[{}] failed to insert relations for blob with id {id}: {e}", self.name);
                return Err(e.into());
            }
        };
        match self.blobs_labels.insert(key, labelsbytes) {
            Ok(_) => {
                log::trace!(target: "mango_chainsaw", "[{}] successfully inserted relation for blob with id {id}", self.name);
                Ok(())
            }
            Err(e) => {
                log::error!(target: "mango_chainsaw", "[{}] failed to insert relation key for blob with id {id}: {e}", self.name);
                Err(e.into())
            }
        }
    }

    /// Upsert a label into the Database.
    ///
    /// This creates, updates, or deletes as necessary
    pub(crate) fn upsert_label(&self, label: &Label, id: u64) -> Result<()> {
        let upsert = |old: Option<&[u8]>| -> Option<Vec<u8>> {
            match old {
                Some(bytes) => {
                    let mut ids: Vec<u64> = match bincode::deserialize::<Vec<u64>>(bytes) {
                        Ok(h) => {
                            log::trace!(target: "mango_chainsaw", "[{}] got {} ids for label {label}", self.name, h.len());
                            h
                        }
                        Err(e) => {
                            log::error!(target: "mango_chainsaw", "[{}] failed to upsert label {label}: {e}", self.name);
                            vec![]
                        }
                    };
                    if !ids.is_empty() {
                        if ids.contains(&id) {
                            // This id already exists in the list. Delete it instead
                            ids.retain(|item| item != &id);
                        } else {
                            ids.push(id);
                        }
                        ids.sort();
                        if let Ok(bs) = bincode::serialize(&ids) {
                            return Some(bs);
                        }
                        return None;
                    }
                    None
                }
                None => {
                    if let Ok(bs) = bincode::serialize(&vec![id]) {
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
        match self.blobs.get(bincode::serialize(&format!("{id}"))?) {
            Ok(Some(blob)) => Ok(Some(Bytes::from(blob.to_vec()))),
            Ok(None) => Ok(None),
            Err(e) => {
                log::error!(target: "mango_chainsaw", "[{}] error getting id={id}: {e}", self.name);
                Err(e.into())
            }
        }
    }

    /// Get all objects matching the given labels
    pub fn query(&self, labels: Vec<Label>) -> Result<Vec<u64>> {
        let mut sets: Vec<HashSet<u64>> = labels.par_iter().map(|label| {
            let labels = self.labels.clone();
            match labels.get(label.key()) {
                Ok(Some(bytes)) => {
                    let ids: Vec<u64> = match bincode::deserialize::<Vec<u64>>(&bytes) {
                        Ok(h) => {
                            log::debug!(target: "mango_chainsaw", "[{}] found {} matches for {label}", self.name, h.len());
                            h
                        },
                        Err(e) => {
                            log::error!(target: "mango_chainsaw", "[{}] failed to deserialize bytes for label {label}: {e}", self.name);
                            vec![]
                        },
                    };
                    ids
                },
                Ok(None) => {
                    log::debug!(target: "mango_chainsaw", "[{}] found no matches for {label}", self.name);
                    vec![]
                },
                Err(e) => {
                    log::error!(target: "mango_chainsaw", "[{}] failed to get {label}: {e}", self.name);
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
        let mut matches = Vec::from_iter(&mut intersection.iter().copied());
        matches.sort();
        Ok(matches)
    }

    /// Delete objects with the given ids.
    pub fn delete_blob(&self, id: u64) -> Result<()> {
        match self.blobs.remove(bincode::serialize(&format!("{id}"))?) {
            Ok(Some(_)) => {
                log::debug!(target: "mango_chainsaw", "[{}] removed blob {id}", self.name);
            }
            Ok(None) => {
                log::warn!(target: "mango_chainsaw", "[{}] got None when removing blob {id}", self.name);
            }
            Err(e) => {
                log::error!(target: "mango_chainsaw", "[{}] failed to remove blob {id}: {e}", self.name);
                return Err(e.into());
            }
        }
        self.cleanup(id)?;
        Ok(())
    }

    /// Clean up after deleted blobs
    ///
    /// This walks the relations tree. Entries that do not have a blob have label data that needs to be cleaned up after.
    pub(crate) fn cleanup(&self, id: u64) -> Result<()> {
        let key = bincode::serialize(&id)?;
        log::info!(target: "mango_chainsaw", "into cleanup");
        let labels: Vec<String> = match self.blobs_labels.get(key) {
            Ok(Some(bytes)) => bincode::deserialize(&bytes)?,
            Ok(None) => vec![],
            Err(e) => {
                log::error!(target: "mango_chainsaw", "[{}] failed to clean up relation for blob {id}: {e}", self.name);
                return Err(e.into());
            }
        };
        let res: Vec<String> = labels.into_par_iter().filter_map(|label| {
            log::info!(target: "mango_chainsaw", "into iter {label}");
            let key = match bincode::serialize(&label) {
                Ok(bytes) => bytes,
                Err(e) => {
                    log::error!(target: "mango_chainsaw", "[{}] failed to serialize relation key for blob {id}: {e}", self.name);
                    return None
                },
            };

            // Get the blob ids for this label
            let mut ids: Vec<u64> = match self.labels.get(&key) {
                Ok(Some(bytes)) => match bincode::deserialize::<Vec<u64>>(&bytes) {
                    Ok(h) => {
                        log::debug!(target: "mango_chainsaw", "[{}] got {} ids for label {label}", self.name, h.len());
                        h
                    },
                    Err(e) => {
                        log::error!(target: "mango_chainsaw", "[{}] failed to deserialize label {label}: {e}", self.name);
                        return Some(label)
                    },
                },
                Ok(None) => {
                    log::error!(target: "mango_chainsaw", "[{}] found no label {label}", self.name);
                    return Some(label)
                },
                Err(e) => {
                    log::error!(target: "mango_chainsaw", "[{}] failed to get label {label}: {e}", self.name);
                    return Some(label)
                },
            };
            // remove our target id
            ids.retain(|item| item != &id);
            ids.sort();

            // If this label is empty, delete it
            if ids.is_empty() {
                match self.labels.remove(&key) {
                    Ok(_) => log::info!(target: "mango_chainsaw", "[{}] removed unused label {label}", self.name),
                    Err(e) => {
                        log::error!(target: "mango_chainsaw", "[{}] failed to remove unused label {label}: {e}", self.name);
                        return Some(label)
                    },
                }
            } else {
                // If the label still has ids, store it
                let bytes = match bincode::serialize(&ids) {
                    Ok(bs) => bs,
                    Err(e) => {
                        log::error!(target: "mango_chainsaw", "[{}] failed to serialize label {label}: {e}", self.name);
                        return Some(label)
                    },
                };
                match self.labels.insert(&key, bytes) {
                    Ok(_) => log::debug!(target: "mango_chainsaw", "[{}] updated label {label} with new ids list", self.name),
                    Err(e) => {
                        log::error!(target: "mango_chainsaw", "[{}] failed to update label {label}: {e}", self.name);
                        return Some(label)
                    },
                }
            }
            None
        }).collect();
        // Labels still in this list had a problem during cleanup
        if res.is_empty() {
            Ok(())
        } else {
            log::error!(target: "mango_chainsaw", "[{}] some labels failed during cleanup: {res:#?}", self.name);
            Ok(())
        }
    }

    /// Get namespace stats
    pub fn stats(&self) -> Result<NamespaceStats> {
        let _prefix = self.name.to_string();
        let stats = NamespaceStats {
            name: self.name.to_string(),
            blob_checksum: self.blobs.checksum()?,
            labels_checksum: self.labels.checksum()?,
            relations_checksum: self.blobs_labels.checksum()?,
            blobs_count: self.blobs.len(),
            labels_count: self.labels.len(),
            relations_count: self.blobs_labels.len(),
        };
        Ok(stats)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, ToResponse)]
pub struct NamespaceStats {
    pub name: String,
    pub blob_checksum: u32,
    pub labels_checksum: u32,
    pub relations_checksum: u32,
    pub blobs_count: usize,
    pub relations_count: usize,
    pub labels_count: usize,
}
