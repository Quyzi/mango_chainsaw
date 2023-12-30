use crate::bucket::Bucket;
use anyhow::{anyhow, Result};
use sled::transaction::{
    ConflictableTransactionError, TransactionalTree, UnabortableTransactionError,
};
use sled::Transactional;
use std::cell::RefCell;

use super::delete::DeleteRequest;
use super::error::*;
use super::execute::ExecuteTransaction;
use super::find::FindRequest;
use super::insert::InsertRequest;

#[derive(Clone)]
pub enum Request {
    Insert(InsertRequest),
    Delete(DeleteRequest),
    Find(FindRequest),
}

#[derive(Clone)]
pub enum RequestResult<'a> {
    Insert(
        InsertRequest,
        std::result::Result<
            <InsertRequest as ExecuteTransaction<'a>>::Output,
            <InsertRequest as ExecuteTransaction<'a>>::Error,
        >,
    ),
    Delete(
        DeleteRequest,
        std::result::Result<
            <DeleteRequest as ExecuteTransaction<'a>>::Output,
            <DeleteRequest as ExecuteTransaction<'a>>::Error,
        >,
    ),
    Find(
        FindRequest,
        std::result::Result<
            <FindRequest as ExecuteTransaction<'a>>::Output,
            <FindRequest as ExecuteTransaction<'a>>::Error,
        >,
    ),
}

impl<'a> ExecuteTransaction<'a> for Request {
    type Error = UnabortableTransactionError;
    type Output = RequestResult<'a>;

    fn execute(
        &self,
        lbl: &'a TransactionalTree,
        lbl_invert: &'a TransactionalTree,
        obj: &'a TransactionalTree,
        obj_lbl: &'a TransactionalTree,
        lbl_obj: &'a TransactionalTree,
    ) -> Result<Self::Output, Self::Error> {
        match self {
            Request::Insert(r) => {
                let inner = r.execute(lbl, lbl_invert, obj, obj_lbl, lbl_obj);
                match inner {
                    Ok(_) => Ok(RequestResult::Insert(r.clone(), inner)),
                    Err(e) => Err(e),
                }
            }
            Request::Delete(r) => {
                let inner = r.execute(lbl, lbl_invert, obj, obj_lbl, lbl_obj);
                match inner {
                    Ok(_) => Ok(RequestResult::Delete(r.clone(), inner)),
                    Err(e) => Err(e),
                }
            }
            Request::Find(r) => {
                let inner = r.execute(lbl, lbl_invert, obj, obj_lbl, lbl_obj);
                match inner {
                    Ok(_) => Ok(RequestResult::Find(r.clone(), inner)),
                    Err(e) => Err(e),
                }
            }
        }
    }
}

pub struct Transaction<'a> {
    pub(crate) namespace: Bucket,
    pub(crate) reqs: RefCell<Vec<Request>>,
    pub(crate) results: RefCell<Vec<RequestResult<'a>>>,
    pub(crate) completed: RefCell<bool>,
}

impl<'a> Transaction<'a> {
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

impl<'a> From<&Bucket> for Transaction<'a> {
    fn from(value: &Bucket) -> Self {
        Self {
            namespace: value.clone(),
            reqs: RefCell::new(vec![]),
            results: RefCell::new(vec![]),
            completed: RefCell::new(false),
        }
    }
}
