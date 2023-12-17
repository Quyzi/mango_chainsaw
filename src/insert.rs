use anyhow::anyhow;
use anyhow::Result;
use bytes::Bytes;
use flexbuffers::FlexbufferSerializer;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sled::transaction::ConflictableTransactionError;
use sled::transaction::UnabortableTransactionError;
use sled::Transactional;
use std::cell::RefCell;
use std::fmt::Display;
use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::{Hash, Hasher},
    sync::Arc,
};
use thiserror::Error;

use crate::common::*;
use crate::{db::Db, namespace::Namespace};

/// A Query Error
#[derive(Debug, Clone, Error)]
pub enum QueryError {
    /// The query has already been executed.
    ///
    /// A query can only be executed once, success or fail.
    AlreadyExecuted,

    /// Something else happened.
    ///
    /// What?
    Undefined,
}

impl Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::AlreadyExecuted => write!(f, "Insert Query Already Executed"),
            _ => write!(f, "Undefined"),
        }
    }
}

/// A `InsertRequest`
///
/// An `InsertRequest` is a request to insert an `Object` into a `Namespace`.
/// It contains a payload, the `ObjectID`, and a list of `Label`s describing it.
#[derive(Clone, Default)]
pub struct InsertRequest {
    pub id: ObjectID,
    pub(crate) obj: Object,
    pub labels: RefCell<HashSet<Label>>,
    pub executed: RefCell<bool>,
}

impl InsertRequest {
    /// Create a new InsertRequest using the hash of the payload as the object ID
    pub fn new(payload: Bytes) -> Self {
        Self {
            id: {
                let mut hasher = DefaultHasher::new();
                payload.hash(&mut hasher);
                hasher.finish()
            },
            obj: Arc::new(payload),
            labels: RefCell::new(HashSet::new()),
            executed: RefCell::new(false),
        }
    }

    /// Create a new InsertRequest with a custom id
    pub fn new_custom_id(id: ObjectID, payload: Bytes) -> Result<Self> {
        let mut this = Self::new(payload);
        this.id = id;
        Ok(this)
    }

    /// Create a new InsertRequest using a monotonic counter to generate the object ID
    pub fn new_using_db(db: &Db, payload: Bytes) -> Result<Self> {
        let mut this = Self::new(payload);
        this.id = db.next_id()?;
        Ok(this)
    }

    /// Add a `Label` to this `InsertRequest`
    pub fn add_label(&self, label: Label) -> Result<()> {
        if self.is_executed()? {
            return Err(anyhow!(QueryError::AlreadyExecuted));
        }
        let mut labels = self.labels.try_borrow_mut()?;
        labels.insert(label);
        Ok(())
    }

    /// Has this `InsertRequest` been executed?
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

    /// Execute this insert request on a `Namespace`
    ///
    /// This inserts the `Object` and its `Label`s into the `Namespace`.
    /// `Label`s are updated or created as necessary.
    /// `InsertRequest`s are transactional.
    pub fn execute(self, ns: &Namespace) -> Result<ObjectID> {
        let labels = &ns.labels;
        let slebal = &ns.labels_inverse;
        let data = &ns.data;
        let data_labels = &ns.data_labels;
        let slebal_atad = &ns.data_labels_inverse;

        if !self.is_executed()? {
            let mut executed = self.executed.try_borrow_mut()?;
            *executed = true;
        } else {
            return Err(anyhow!(QueryError::AlreadyExecuted));
        }

        (labels, slebal, data, data_labels, slebal_atad)
            .transaction(
                |(tx_labels, tx_slebal, tx_data, tx_data_labels, tx_slebal_atad)| {
                    let object_id_bytes = Self::ser(self.id)?;

                    // Insert the data
                    tx_data.insert(object_id_bytes.clone(), Self::ser(&*self.obj)?)?;
                    log::info!(
                        target: "mango_chainsaw::insert::execute",
                        "inserted object with id {id}",
                        id = &self.id,
                    );

                    // Collect label ids
                    let mut label_ids = vec![];
                    let request_labels = self
                        .labels
                        .try_borrow()
                        .map_err(|_e| UnabortableTransactionError::Conflict)?;

                    // Insert the labels and labels_inverse values
                    for label in request_labels.clone() {
                        let id = label.id();
                        let key_bytes = Self::ser(id)?;
                        let struct_bytes = Self::ser(label.clone())?;
                        let value_bytes = label.data.as_bytes();
                        tx_labels.insert(key_bytes.clone(), struct_bytes)?;
                        tx_slebal.insert(value_bytes, key_bytes)?;
                        label_ids.push(id);
                        log::info!(
                            target: "mango_chainsaw::insert::execute",
                            "inserted label with id {id}: {}",
                            label.data
                        );
                    }

                    // Insert data_labels
                    tx_data_labels.insert(object_id_bytes.clone(), Self::ser(&label_ids)?)?;
                    log::info!(
                        target: "mango_chainsaw::insert::execute",
                        "inserted data_labels for id {id}",
                        id = &self.id,
                    );

                    // Upsert data_labels_inverse
                    for id in label_ids {
                        let label_id_bytes = Self::ser(id)?;
                        match tx_slebal_atad.remove(label_id_bytes.clone())? {
                            Some(old) => {
                                let mut object_ids: Vec<ObjectID> = Self::de(old.to_vec())?;
                                object_ids.push(self.id);
                                tx_slebal_atad.insert(
                                    label_id_bytes.clone(),
                                    Self::ser(object_ids.to_owned())?,
                                )?;
                                log::info!(
                                    target: "mango_chainsaw::insert::execute",
                                    "upserted existing data_labels with id {id}: {object_ids:?}",
                                )
                            }
                            None => {
                                tx_slebal_atad
                                    .insert(label_id_bytes.clone(), Self::ser(vec![&self.id])?)?;
                                log::info!(
                                    target: "mango_chainsaw::insert::execute",
                                    "inserted new data_labels with id {id}",
                                )
                            }
                        }
                    }
                    Ok::<(), ConflictableTransactionError<String>>(())
                },
            )
            .map_err(|e| anyhow!("{}", e))?;
        Ok(self.id)
    }
}
