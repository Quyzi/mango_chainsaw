use flexbuffers::FlexbufferSerializer;
use sled::transaction::UnabortableTransactionError;
use std::cell::RefCell;

use crate::{label::Label, object::ObjectID};

use super::transaction::ExecuteTransaction;

#[derive(Clone, Debug)]
pub struct DeleteRequest {
    /// A List of ObjectIDs to delete
    objects: RefCell<Vec<ObjectID>>,

    /// Prune unused labels
    ///
    /// Default: true
    prune: RefCell<bool>,
}

impl From<Vec<ObjectID>> for DeleteRequest {
    fn from(ids: Vec<ObjectID>) -> Self {
        Self {
            objects: RefCell::new(ids),
            prune: RefCell::new(true),
        }
    }
}

impl DeleteRequest {}

impl<'a> ExecuteTransaction<'a> for DeleteRequest {
    type Error = UnabortableTransactionError;
    type Output = Vec<(ObjectID, bool)>;

    fn transaction_ser<T: serde::Serialize>(item: T) -> anyhow::Result<bytes::Bytes, Self::Error> {
        let mut s = FlexbufferSerializer::new();
        match item.serialize(&mut s) {
            Ok(_) => Ok(s.take_buffer().into()),
            Err(e) => Err(UnabortableTransactionError::Storage(sled::Error::Io(
                std::io::Error::other(e),
            ))),
        }
    }

    fn transaction_de<T: serde::de::DeserializeOwned>(
        bytes: bytes::Bytes,
    ) -> anyhow::Result<T, Self::Error> {
        flexbuffers::from_slice(&bytes).map_err(|e| {
            UnabortableTransactionError::Storage(sled::Error::Io(std::io::Error::other(e)))
        })
    }

    fn execute(
        &self,
        lbl: &'a sled::transaction::TransactionalTree,
        lbl_invert: &'a sled::transaction::TransactionalTree,
        obj: &'a sled::transaction::TransactionalTree,
        obj_lbl: &'a sled::transaction::TransactionalTree,
        lbl_obj: &'a sled::transaction::TransactionalTree,
    ) -> anyhow::Result<Self::Output, Self::Error> {
        let mut results = vec![];

        let ids = self
            .objects
            .try_borrow()
            .map_err(|e| {
                UnabortableTransactionError::Storage(sled::Error::Io(std::io::Error::other(e)))
            })?
            .clone();

        let prune = *self.prune.try_borrow().map_err(|e| {
            UnabortableTransactionError::Storage(sled::Error::Io(std::io::Error::other(e)))
        })?;

        for id in ids {
            let key_bytes = Self::transaction_ser(id)?;
            // delete the object itself
            let removed = {
                match obj.remove(key_bytes.clone().to_vec()) {
                    Ok(Some(old)) => {
                        log::trace!("removed object with id {id} size: {}b", old.len());
                        true
                    }
                    Ok(None) => {
                        log::trace!("failed to remove object with id {id}: object not found");
                        false
                    }
                    Err(e) => {
                        log::error!("error removing object with id {id}: {e}");
                        false
                    }
                }
            };

            if !removed {
                continue;
            }

            // if the object was removed, find its labels
            let labels = {
                match obj_lbl.remove(key_bytes.clone().to_vec()) {
                    Ok(Some(thing)) => {
                        let this = Self::transaction_de::<Vec<String>>(thing.to_vec().into())?;
                        log::trace!(
                            "found list of {} labels for object with id {id}",
                            this.len()
                        );
                        this
                    }
                    Ok(None) => {
                        log::trace!("found no labels for object with id {id}");
                        vec![]
                    }
                    Err(e) => {
                        log::error!("error getting labels for object with id {id}: {e}");
                        return Err(e);
                    }
                }
            };

            // Remove the object id from the label
            // Optionally remove the label if it is no longer being used (default: true)
            for label in labels {
                let label = match Label::try_from(label) {
                    Ok(this) => this,
                    Err(e) => {
                        return Err(UnabortableTransactionError::Storage(
                            sled::Error::Unsupported(e.to_string()),
                        ))
                    }
                };
                let key_bytes = Self::ser_label(label.clone())?;

                // Get the list of objectIDs described by the label
                match lbl_obj.remove(key_bytes.to_vec())? {
                    Some(bytes) => {
                        let old = Self::transaction_de::<Vec<ObjectID>>(bytes.to_vec().into())?;
                        let new = old
                            .into_iter()
                            .filter(|i| i != &id)
                            .collect::<Vec<ObjectID>>();

                        // Remove unused labels
                        if new.is_empty() && prune {
                            let _ = lbl.remove(key_bytes.to_vec())?;
                            let _ = lbl_invert.remove(key_bytes.to_vec())?;
                            log::trace!("removed unused label {}", label.to_string_ltr());
                            continue;
                        }

                        // Add back the updated list with this objectID removed
                        let val_bytes = Self::transaction_ser(new)?;
                        lbl.insert(key_bytes.to_vec(), val_bytes.to_vec())?;
                        log::trace!("updated label {}", label.to_string_ltr())
                    }
                    None => {
                        log::error!("found no label {}", label.to_string_ltr());
                    }
                }
            }

            results.push((id, true))
        }

        Ok(results)
    }
}
