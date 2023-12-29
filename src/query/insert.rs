use anyhow::Result;
use bytes::Bytes;
use flexbuffers::FlexbufferSerializer;
use sled::transaction::{TransactionalTree, UnabortableTransactionError};
use std::{cell::RefCell, io};

use crate::label::SEPARATOR;
use crate::mango::Mango;
use crate::{
    label::Label,
    object::{Object, ObjectID},
};

use super::transaction::ExecuteTransaction;

#[derive(Clone, Debug)]
pub struct InsertRequest {
    pub(crate) object: Object,
    pub(crate) id: RefCell<ObjectID>,
    pub(crate) labels: RefCell<Vec<Label>>,
}

impl InsertRequest {
    pub fn new(object: Bytes) -> Result<Self> {
        Ok(object.into())
    }

    pub fn new_static_id(id: ObjectID, object: Bytes) -> Result<Self> {
        let this: Self = object.into();
        this.set_id(id)?;
        Ok(this)
    }

    pub fn new_monotonic_id(mango: &Mango, object: Bytes) -> Result<Self> {
        let id = mango.inner.generate_id()?;
        Self::new_static_id(id, object)
    }

    pub fn add_label(&self, label: Label) -> Result<usize> {
        let mut labels = self.labels.try_borrow_mut()?;
        labels.push(label);
        Ok(labels.len())
    }

    pub fn add_labels(&self, labels: Vec<Label>) -> Result<usize> {
        let mut my_labels = self.labels.try_borrow_mut()?;
        my_labels.extend(labels);
        my_labels.sort();
        my_labels.dedup();
        Ok(my_labels.len())
    }

    pub fn set_id(&self, new: ObjectID) -> Result<ObjectID> {
        let mut id = self.id.try_borrow_mut()?;
        let old = *id;
        *id = new;
        Ok(old)
    }
}

impl From<Bytes> for InsertRequest {
    fn from(value: Bytes) -> Self {
        Self {
            object: value.into(),
            id: RefCell::new(0),
            labels: RefCell::new(vec![]),
        }
    }
}

impl<'a> ExecuteTransaction<'a> for InsertRequest {
    type Error = UnabortableTransactionError;
    type Output = ObjectID;

    fn transaction_ser<T: serde::Serialize>(item: T) -> Result<Bytes, Self::Error> {
        let mut s = FlexbufferSerializer::new();
        match item.serialize(&mut s) {
            Ok(_) => Ok(s.take_buffer().into()),
            Err(e) => Err(UnabortableTransactionError::Storage(sled::Error::Io(
                io::Error::other(e),
            ))),
        }
    }

    fn transaction_de<T: serde::de::DeserializeOwned>(bytes: Bytes) -> Result<T, Self::Error> {
        flexbuffers::from_slice(&bytes)
            .map_err(|e| UnabortableTransactionError::Storage(sled::Error::Io(io::Error::other(e))))
    }

    fn ser_label(label: Label) -> Result<Bytes, Self::Error> {
        Self::transaction_ser(format!("{}{}{}", label.0, SEPARATOR, label.1))
    }

    fn ser_label_invert(label: Label) -> Result<Bytes, Self::Error> {
        Self::transaction_ser(format!("{}{}{}", label.1, SEPARATOR, label.0))
    }

    fn execute(
        &self,
        lbl: &'a TransactionalTree,
        lbl_invert: &'a TransactionalTree,
        obj: &'a TransactionalTree,
        obj_lbl: &'a TransactionalTree,
        lbl_obj: &'a TransactionalTree,
    ) -> Result<Self::Output, Self::Error> {
        let object_id = *self.id.try_borrow().map_err(|e| {
            UnabortableTransactionError::Storage(sled::Error::Io(io::Error::other(e)))
        })?;
        let labels = self
            .labels
            .try_borrow()
            .map_err(|e| {
                UnabortableTransactionError::Storage(sled::Error::Io(io::Error::other(e)))
            })?
            .clone();

        // Insert the object
        {
            let key_bytes = Self::transaction_ser(object_id)?;
            let val_bytes = Self::transaction_ser(self.object.get_inner())?;
            obj.insert(key_bytes.to_vec(), val_bytes.to_vec())?;
            log::trace!("Inserted bytes for object with id {object_id}");
        }

        let mut label_ids = vec![];
        for label in labels {
            label_ids.push(label.to_string_ltr());

            // Insert key=value to labels tree
            {
                let key_bytes = Self::ser_label(label.clone())?;
                let val_bytes = Self::transaction_ser(label.clone())?;
                lbl.insert(key_bytes.to_vec(), val_bytes.to_vec())?;
                log::trace!("Inserted label {} into labels", label.to_string_ltr());
            }

            // Insert value=key to labels invert tree
            {
                let key_bytes = Self::ser_label_invert(label.clone())?;
                let val_bytes = Self::transaction_ser(label.clone())?;
                lbl_invert.insert(key_bytes.to_vec(), val_bytes.to_vec())?;
                log::trace!(
                    "Inserted label {} into labels_inverse",
                    label.to_string_rtl()
                )
            }

            // Upsert this object id into this label in the objects labels invert tree
            {
                let key_bytes = Self::ser_label(label.clone())?;
                match lbl_obj.get(&key_bytes.clone()) {
                    Ok(Some(thing)) => {
                        let mut objects: Vec<ObjectID> =
                            Self::transaction_de(Bytes::from(thing.to_vec()))?;
                        objects.push(object_id);
                        let val_bytes = Self::transaction_ser(objects)?;
                        lbl_obj.insert(key_bytes.to_vec(), val_bytes.to_vec())?;
                        log::trace!(
                            "Upserted object id {object_id} into label {}",
                            label.to_string_ltr()
                        );
                    }
                    Ok(None) => {
                        let val_bytes = Self::transaction_ser(vec![object_id])?;
                        lbl_obj.insert(key_bytes.to_vec(), val_bytes.to_vec())?;
                        log::trace!(
                            "Inserted object id {object_id} into new label {}",
                            label.to_string_ltr()
                        );
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        // Add object id = [labels] to objects labels tree
        {
            let key_bytes = Self::transaction_ser(object_id)?;
            let val_bytes = Self::transaction_ser(label_ids)?;
            obj_lbl.insert(key_bytes.to_vec(), val_bytes.to_vec())?;
            log::trace!("Inserted labels for object with id {object_id} into objects_labels tree.");
        }

        Ok(object_id)
    }
}
