use crate::object::{Object, ObjectID};
use anyhow::Result;
use bytes::Bytes;
use sled::transaction::UnabortableTransactionError;
use std::cell::RefCell;

use super::execute::ExecuteTransaction;

#[derive(Clone, Debug)]
pub struct GetRequest {
    ids: RefCell<Vec<ObjectID>>,
}

impl GetRequest {
    pub fn new(ids: Vec<ObjectID>) -> Result<Self> {
        Ok(Self {
            ids: RefCell::new(ids),
        })
    }

    pub fn add_id(&self, id: ObjectID) -> Result<usize> {
        let mut ids = self.ids.try_borrow_mut()?;
        ids.push(id);
        ids.sort();
        ids.dedup();
        Ok(ids.len())
    }

    pub fn set_ids(&self, ids: Vec<ObjectID>) -> Result<usize> {
        let mut my_ids = self.ids.try_borrow_mut()?;
        *my_ids = ids;
        my_ids.sort();
        my_ids.dedup();
        Ok(my_ids.len())
    }
}

impl ExecuteTransaction for GetRequest {
    type Error = UnabortableTransactionError;
    type Output = Vec<(ObjectID, Bytes)>;

    fn execute(
        &self,
        _lbl: &sled::transaction::TransactionalTree,
        _ilbl: &sled::transaction::TransactionalTree,
        obj: &sled::transaction::TransactionalTree,
        _objlbl: &sled::transaction::TransactionalTree,
        _objilbl: &sled::transaction::TransactionalTree,
    ) -> std::prelude::v1::Result<Self::Output, Self::Error> {
        let ids = self.ids.take();

        let mut results = vec![];
        for id in ids {
            let key_bytes = Self::transaction_ser(id)?;
            match obj.get(&key_bytes) {
                Ok(Some(bytes)) => {
                    let obj = Object::try_from(bytes).map_err(|e| {
                        UnabortableTransactionError::Storage(sled::Error::Unsupported(
                            e.to_string(),
                        ))
                    })?;
                    results.push((id, obj.get_inner()))
                }
                Ok(None) => results.push((id, Bytes::new())),
                Err(e) => {
                    log::error!("error getting object with id {id}: {e}");
                    return Err(e);
                }
            }
        }

        Ok(results)
    }
}
