use crate::common::*;
use crate::namespace::Namespace;
use anyhow::anyhow;
use anyhow::Result;
use flexbuffers::FlexbufferSerializer;
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sled::transaction::ConflictableTransactionError;
use sled::transaction::UnabortableTransactionError;
use sled::Transactional;
use std::{cell::RefCell, collections::HashSet, fmt::Display};
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum QueryError {
    AlreadyExecuted,
    Undefined,
}

impl Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::AlreadyExecuted => write!(f, "Delete Query Already Executed"),
            _ => write!(f, "Undefined"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DeleteRequest {
    objects: RefCell<HashSet<ObjectID>>,
    executed: RefCell<bool>,
}

impl Default for DeleteRequest {
    fn default() -> Self {
        Self {
            objects: RefCell::new(HashSet::new()),
            executed: RefCell::new(false),
        }
    }
}

impl DeleteRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_object(&self, id: ObjectID) -> Result<()> {
        if self.is_executed()? {
            return Err(anyhow!(QueryError::AlreadyExecuted));
        }
        let mut objects = self.objects.try_borrow_mut()?;
        objects.insert(id);
        Ok(())
    }

    pub fn is_executed(&self) -> Result<bool> {
        Ok(*self.executed.try_borrow()?)
    }

    /// Helper serialization fn to serialize a thing inside a transaction block
    pub(crate) fn ser<T: Serialize>(thing: T) -> Result<Vec<u8>, UnabortableTransactionError> {
        let mut s = FlexbufferSerializer::new();
        thing.serialize(&mut s).map_err(|e| {
            UnabortableTransactionError::Storage(sled::Error::Io(std::io::Error::other(e)))
        })?;
        Ok(s.take_buffer())
    }

    /// Helper deserialization fn to serialize a thing inside a transaction block
    pub(crate) fn de<T: DeserializeOwned>(
        bytes: Vec<u8>,
    ) -> Result<T, UnabortableTransactionError> {
        let this = flexbuffers::from_slice(&bytes).map_err(|e| {
            UnabortableTransactionError::Storage(sled::Error::Io(std::io::Error::other(e)))
        })?;
        Ok(this)
    }

    pub fn execute(&self, ns: Namespace) -> Result<()> {
        let labels = &ns.labels;
        let slebal = &ns.labels_inverse;
        let data = &ns.data;
        let data_labels = &ns.data_labels;
        let slebal_atad = &ns.data_labels_inverse;

        {
            let mut executed = self.executed.try_borrow_mut()?;
            *executed = true;
        }

        let req_objects = self.objects.take();

        (labels, slebal, data, data_labels, slebal_atad)
            .transaction(
                |(tx_labels, tx_slebal, tx_data, tx_data_labels, tx_slebal_atad)| {
                    for object_id in &req_objects {
                        let id = Self::ser(object_id)?;

                        // Remove the object from the data tree
                        tx_data.remove(id.clone())?;

                        // Get the labels attached to this object
                        let object_labels: Vec<LabelID> = match tx_data_labels.remove(id.clone())? {
                            Some(bs) => Self::de(bs.to_vec())?,
                            None => vec![],
                        };

                        // Remove the current object_id from each label
                        for label in object_labels {
                            let label_id = Self::ser(label)?;
                            if let Some(object_ids_bs) = tx_slebal_atad.remove(label_id.clone())? {
                                let object_ids: Vec<ObjectID> = Self::de(object_ids_bs.to_vec())?;
                                if object_ids.len() == 1 || object_ids.is_empty() {
                                    // If this label has only one object it can be removed
                                    tx_labels.remove(label_id.clone())?;
                                    tx_slebal.remove(Self::ser(label)?)?;
                                    continue;
                                }

                                // Remove the current object_id from the list and add it back
                                let new_ids: Vec<ObjectID> = object_ids
                                    .into_par_iter()
                                    .filter(|id| id != object_id)
                                    .collect();

                                tx_slebal_atad.insert(label_id, Self::ser(new_ids)?)?;
                            }
                        }
                    }
                    Ok::<(), ConflictableTransactionError<String>>(())
                },
            )
            .map_err(|e| anyhow!("{}", e))?;

        Ok(())
    }
}
