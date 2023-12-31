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
use super::get::GetRequest;
use super::insert::InsertRequest;

#[derive(Clone)]
pub enum Request {
    Insert(InsertRequest),
    Delete(DeleteRequest),
    Find(FindRequest),
    Get(GetRequest),
}

impl From<InsertRequest> for Request {
    fn from(value: InsertRequest) -> Self {
        Self::Insert(value)
    }
}
impl From<DeleteRequest> for Request {
    fn from(value: DeleteRequest) -> Self {
        Self::Delete(value)
    }
}
impl From<FindRequest> for Request {
    fn from(value: FindRequest) -> Self {
        Self::Find(value)
    }
}
impl From<GetRequest> for Request {
    fn from(value: GetRequest) -> Self {
        Self::Get(value)
    }
}

#[derive(Clone, Debug)]
pub enum RequestResult {
    Insert(
        Box<InsertRequest>,
        std::result::Result<
            <InsertRequest as ExecuteTransaction>::Output,
            <InsertRequest as ExecuteTransaction>::Error,
        >,
    ),
    Delete(
        Box<DeleteRequest>,
        std::result::Result<
            <DeleteRequest as ExecuteTransaction>::Output,
            <DeleteRequest as ExecuteTransaction>::Error,
        >,
    ),
    Find(
        Box<FindRequest>,
        std::result::Result<
            <FindRequest as ExecuteTransaction>::Output,
            <FindRequest as ExecuteTransaction>::Error,
        >,
    ),
    Get(
        Box<GetRequest>,
        std::result::Result<
            <GetRequest as ExecuteTransaction>::Output,
            <GetRequest as ExecuteTransaction>::Error,
        >,
    ),
}

impl ExecuteTransaction for Request {
    type Error = UnabortableTransactionError;
    type Output = RequestResult;

    fn execute(
        &self,
        lbl: &TransactionalTree,
        lbl_invert: &TransactionalTree,
        obj: &TransactionalTree,
        obj_lbl: &TransactionalTree,
        lbl_obj: &TransactionalTree,
    ) -> Result<Self::Output, Self::Error> {
        match self {
            Request::Insert(r) => {
                let inner = r.execute(lbl, lbl_invert, obj, obj_lbl, lbl_obj);
                match inner {
                    Ok(_) => Ok(RequestResult::Insert(Box::new(r.clone()), inner)),
                    Err(e) => Err(e),
                }
            }
            Request::Delete(r) => {
                let inner = r.execute(lbl, lbl_invert, obj, obj_lbl, lbl_obj);
                match inner {
                    Ok(_) => Ok(RequestResult::Delete(Box::new(r.clone()), inner)),
                    Err(e) => Err(e),
                }
            }
            Request::Find(r) => {
                let inner = r.execute(lbl, lbl_invert, obj, obj_lbl, lbl_obj);
                match inner {
                    Ok(_) => Ok(RequestResult::Find(Box::new(r.clone()), inner)),
                    Err(e) => Err(e),
                }
            }
            Request::Get(r) => {
                let inner = r.execute(lbl, lbl_invert, obj, obj_lbl, lbl_obj);
                match inner {
                    Ok(_) => Ok(RequestResult::Get(Box::new(r.clone()), inner)),
                    Err(e) => Err(e),
                }
            }
        }
    }
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

    pub fn results(&self) -> Result<Vec<RequestResult>> {
        let results = self.results.try_borrow()?;
        Ok(results.to_owned())
    }

    pub fn execute(&self) -> Result<()> {
        match self.completed.try_borrow() {
            Ok(c) => match *c {
                true => return Err(TransactionError::AlreadyExecuted.into()),
                false => (),
            },
            Err(e) => return Err(anyhow!(e)),
        }

        let requests = self.reqs.try_borrow()?;

        let results = RefCell::new(vec![]);
        (
            &self.namespace.t_labels,
            &self.namespace.t_labels_invert,
            &self.namespace.t_objects,
            &self.namespace.t_objects_labels,
            &self.namespace.t_labels_objects,
        )
            .transaction(|(tx_lbl, tx_ilbl, tx_obj, tx_objlbl, tx_objilbl)| {
                for (n, req) in requests.iter().enumerate() {
                    let res = req.execute(tx_lbl, tx_ilbl, tx_obj, tx_objlbl, tx_objilbl)?;

                    let mut results = results.try_borrow_mut().map_err(|e| {
                        ConflictableTransactionError::Storage(sled::Error::Unsupported(
                            e.to_string(),
                        ))
                    })?;
                    results.push(res);

                    log::trace!(
                        "completed request {} of {} in transaction",
                        n + 1,
                        requests.len()
                    );
                }
                Ok::<(), ConflictableTransactionError<String>>(())
            })
            .map_err(|e| anyhow!("{}", e))?;

        let mut my_results = self.results.try_borrow_mut()?;
        *my_results = results.take();
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
