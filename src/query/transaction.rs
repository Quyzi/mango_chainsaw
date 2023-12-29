use crate::label::Label;
use crate::object::Object;
use crate::{bucket::Bucket, object::ObjectID};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_derive::{Deserialize as DeDerive, Serialize as SerDerive};
use sled::transaction::{
    ConflictableTransactionError, TransactionalTree, UnabortableTransactionError,
};
use sled::Transactional;
use std::cell::RefCell;
use thiserror::Error;

use super::cswap::CompareSwapRequest;
use super::delete::DeleteRequest;
use super::find::FindRequest;
use super::get::GetRequest;
use super::insert::InsertRequest;

#[derive(Clone)]
pub enum Request {
    Insert(InsertRequest),
    Delete(DeleteRequest),
    CompareSwap(CompareSwapRequest),
    Find(FindRequest),
    Get(GetRequest),
}

impl<'a> ExecuteTransaction<'a> for Request {
    type Error = UnabortableTransactionError;
    type Output = RequestResult;

    fn transaction_ser<T: Serialize>(_item: T) -> Result<Bytes, Self::Error> {
        unreachable!()
    }

    fn transaction_de<T: DeserializeOwned>(_bytes: Bytes) -> Result<T, Self::Error> {
        unreachable!()
    }

    fn execute(
        &self,
        lbl: &'a TransactionalTree,
        lbl_invert: &'a TransactionalTree,
        obj: &'a TransactionalTree,
        obj_lbl: &'a TransactionalTree,
        lbl_obj: &'a TransactionalTree,
    ) -> Result<Self::Output, Self::Error> {
        match self {
            Request::Insert(r) => match r.execute(lbl, lbl_invert, obj, obj_lbl, lbl_obj) {
                Ok(oid) => Ok(RequestResult::Insert(r.clone(), Ok(oid))),
                Err(e) => Ok(RequestResult::Insert(r.clone(), Err(e.into()))),
            },
            Request::Delete(r) => match r.execute(lbl, lbl_invert, obj, obj_lbl, lbl_obj) {
                Ok(results) => Ok(RequestResult::Delete(r.clone(), Ok(results))),
                Err(e) => Ok(RequestResult::Delete(r.clone(), Err(e.into()))),
            },
            Request::CompareSwap(_r) => todo!(),
            Request::Find(_r) => todo!(),
            Request::Get(_g) => todo!(),
        }
    }
}

pub enum RequestResult {
    Insert(InsertRequest, Result<ObjectID>),
    Delete(DeleteRequest, Result<Vec<(u64, bool)>>),
    CompareSwap(CompareSwapRequest, Result<Option<Object>>),
    Find(FindRequest, Result<Vec<ObjectID>>),
    Get(GetRequest, Result<Object>),
}

#[derive(Error, Debug, SerDerive, DeDerive)]
pub enum TransactionError {
    #[error("transaction already executed")]
    AlreadyExecuted,
}

pub struct Transaction {
    pub(crate) namespace: Bucket,
    pub(crate) reqs: RefCell<Vec<Request>>,
    pub(crate) results: RefCell<Vec<RequestResult>>,
    pub(crate) completed: RefCell<bool>,
}

impl Transaction {
    pub fn new(ns: Bucket) -> Self {
        (&ns).into()
    }

    pub fn append_request(&self, req: Request) -> Result<usize> {
        if self.completed()? {
            return Err(TransactionError::AlreadyExecuted.into());
        }

        let mut reqs = self.reqs.try_borrow_mut()?;
        reqs.push(req);
        Ok(reqs.len())
    }

    pub fn reset(&self) -> Result<()> {
        let mut completed = self.completed.try_borrow_mut()?;

        let mut results = self.results.try_borrow_mut()?;
        let reqs = self.reqs.try_borrow()?;
        *results = Vec::with_capacity(reqs.len());

        *completed = false;
        Ok(())
    }

    pub fn completed(&self) -> Result<bool> {
        Ok(*self.completed.try_borrow()?)
    }

    pub fn execute(&self) -> Result<()> {
        match self.completed.try_borrow() {
            Ok(c) => match *c {
                true => return Err(TransactionError::AlreadyExecuted.into()),
                false => (),
            },
            Err(e) => return Err(anyhow!(e)),
        }

        let requests = self.reqs.try_borrow()?.to_vec();

        (
            &self.namespace.t_labels,
            &self.namespace.t_labels_invert,
            &self.namespace.t_objects,
            &self.namespace.t_objects_labels,
            &self.namespace.t_labels_objects,
        )
            .transaction(|(tx_lbl, tx_ilbl, tx_obj, tx_objlbl, tx_objilbl)| {
                for (n, req) in requests.iter().enumerate() {
                    req.execute(tx_lbl, tx_ilbl, tx_obj, tx_objlbl, tx_objilbl)?;
                    log::trace!(
                        "completed request {} of {} in transaction",
                        n + 1,
                        requests.len()
                    );
                }
                Ok::<(), ConflictableTransactionError<String>>(())
            })
            .map_err(|e| anyhow!("{}", e))?;
        Ok(())
    }

    pub fn len(&self) -> Result<usize> {
        let r = self.reqs.try_borrow()?;
        Ok(r.len())
    }

    pub fn is_empty(&self) -> Result<bool> {
        let r = self.reqs.try_borrow()?;
        Ok(r.is_empty())
    }
}

impl From<&Bucket> for Transaction {
    fn from(value: &Bucket) -> Self {
        Self {
            namespace: value.clone(),
            reqs: RefCell::new(vec![]),
            results: RefCell::new(vec![]),
            completed: RefCell::new(false),
        }
    }
}

pub(crate) trait ExecuteTransaction<'a> {
    type Error;
    type Output;

    fn transaction_ser<T: Serialize>(item: T) -> Result<Bytes, Self::Error>;
    fn transaction_de<T: DeserializeOwned>(bytes: Bytes) -> Result<T, Self::Error>;

    fn ser_label(_label: Label) -> Result<Bytes, Self::Error> {
        unreachable!()
    }
    fn ser_label_invert(_label: Label) -> Result<Bytes, Self::Error> {
        unreachable!()
    }
    fn de_label(_bytes: Bytes) -> Result<Label, Self::Error> {
        unreachable!()
    }
    fn de_label_invert(_bytes: Bytes) -> Result<Label, Self::Error> {
        unreachable!()
    }

    fn execute(
        &self,
        lbl: &'a TransactionalTree,
        ilbl: &'a TransactionalTree,
        obj: &'a TransactionalTree,
        objlbl: &'a TransactionalTree,
        objilbl: &'a TransactionalTree,
    ) -> Result<Self::Output, Self::Error>;
}
