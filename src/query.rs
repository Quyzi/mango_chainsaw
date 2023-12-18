use crate::{common::*, namespace::Namespace};
use anyhow::{anyhow, Result};
use flexbuffers::FlexbufferSerializer;
use minitrace::prelude::*;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::{de::DeserializeOwned, Serialize};
use std::{cell::RefCell, collections::HashSet, fmt::Display};
use thiserror::Error;
/// A Query Error
#[derive(Debug, Clone, Error)]
pub enum QueryError {
    /// The query has already been executed.
    ///
    /// A query can onloy be executed once.
    AlreadyExecuted,

    /// The query hasn't been executed yet!
    NotYetExecuted,

    /// Something broke
    ///
    /// Oops
    Undefined,
}

impl Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::AlreadyExecuted => write!(f, "Search Query Already Executed"),
            QueryError::NotYetExecuted => write!(f, "Search Query Not Yet Executed"),
            _ => write!(f, "Undefined"),
        }
    }
}

/// A `QueryRequest`
///
/// Query a `Namespace` for any `Object`s matching a given set of `Label`s.
/// Specific `Label`s can be included or excluded as needed.
/// Executing a `QueryRequest` returns a set of `ObjectID`s matching:
/// 1. Any of the include `Label`s
/// 2. None of the exclude `Label`s
pub struct QueryRequest {
    /// Labels to be included in the query
    pub include_labels: RefCell<HashSet<Label>>,

    /// Labels to be excluded from the query
    pub exclude_labels: RefCell<HashSet<Label>>,

    pub include_prefix: RefCell<HashSet<Label>>,

    /// The results of executing the query on a `Namespace`
    pub results: RefCell<Option<HashSet<ObjectID>>>,

    /// Has this query been executed?
    executed: RefCell<bool>,
}

impl Default for QueryRequest {
    fn default() -> Self {
        Self {
            include_labels: RefCell::new(HashSet::new()),
            exclude_labels: RefCell::new(HashSet::new()),
            include_prefix: RefCell::new(HashSet::new()),
            results: RefCell::new(None),
            executed: RefCell::new(false),
        }
    }
}

impl QueryRequest {
    /// Create a new `QueryRequest`
    pub fn new() -> Self {
        Self::default()
    }

    /// Has this `QueryRequest` been executed?
    pub fn is_executed(&self) -> Result<bool> {
        Ok(*self.executed.try_borrow()?)
    }

    /// Get the results of this query.
    ///
    /// The query needs to have been executed to work.
    pub fn results(&self) -> Result<Option<Vec<ObjectID>>> {
        if !self.is_executed()? {
            return Err(anyhow!(QueryError::NotYetExecuted));
        }
        let results = self
            .results
            .try_borrow()?
            .clone()
            .map(|r| r.into_iter().collect());
        Ok(results)
    }

    /// Include a `Label` in this query
    pub fn include(&self, label: Label) -> Result<()> {
        if self.is_executed()? {
            return Err(anyhow!(QueryError::AlreadyExecuted));
        }
        let mut labels = self.include_labels.try_borrow_mut()?;
        labels.insert(label);
        Ok(())
    }

    /// Add a prefix scan `Label`
    pub fn include_prefix(&self, label: Label) -> Result<()> {
        if self.is_executed()? {
            return Err(anyhow!(QueryError::AlreadyExecuted));
        }
        let mut include_prefix = self.include_prefix.try_borrow_mut()?;
        include_prefix.insert(label);
        Ok(())
    }

    /// Exclude a `Label` from this query
    pub fn exclude(&self, label: Label) -> Result<()> {
        if self.is_executed()? {
            return Err(anyhow!(QueryError::AlreadyExecuted));
        }
        let mut labels = self.exclude_labels.try_borrow_mut()?;
        labels.insert(label);
        Ok(())
    }

    /// Helper serialization fn to serialize a thing
    pub(crate) fn ser<T: Serialize>(thing: T) -> Result<Vec<u8>> {
        let mut s = FlexbufferSerializer::new();
        thing.serialize(&mut s)?;
        Ok(s.take_buffer())
    }

    /// Helper deserialization fn to serialize a thing
    pub(crate) fn de<T: DeserializeOwned>(bytes: Vec<u8>) -> Result<T> {
        let this = flexbuffers::from_slice(&bytes)?;
        Ok(this)
    }

    /// Execute this query on a `Namespace`
    ///
    /// * Include `Label`s use OR logic
    ///
    /// This will return a set of `ObjectID`s that match:
    /// 1. Any of the include `Label`s
    /// 2. None of the exclude `Label`s
    #[trace]
    pub fn execute(&self, ns: &Namespace) -> Result<Vec<ObjectID>> {
        if self.is_executed()? {
            return Err(anyhow!(QueryError::AlreadyExecuted));
        }

        let slebal_atad = &ns.data_labels_inverse;
        let slebal = &ns.labels_inverse;

        if !self.is_executed()? {
            let mut executed = self.executed.try_borrow_mut()?;
            *executed = true;
        } else {
            return Err(anyhow!(QueryError::AlreadyExecuted));
        }

        let includes = self.include_labels.try_borrow()?.clone();
        let excludes = self.exclude_labels.try_borrow()?.clone();
        let include_prefixes = self.include_prefix.try_borrow()?.clone();

        let mut all_includes: HashSet<Label> = includes;
        for label in include_prefixes {
            let prefix = label.data.as_bytes();
            let mut scanner = slebal.scan_prefix(prefix);
            while let Some(Ok((bytes, _))) = scanner.next() {
                let lbl: String = String::from_utf8(bytes.to_vec())?;
                all_includes.insert(Label::new(&lbl));
            }
        }

        let mut include_object_ids: HashSet<ObjectID> = HashSet::new();
        for label in all_includes {
            match slebal_atad.get(Self::ser(label.id())?) {
                Ok(Some(bs)) => {
                    let object_ids: Vec<ObjectID> = Self::de(bs.to_vec())?;
                    include_object_ids.extend(object_ids.iter());
                }
                Ok(None) => {}
                Err(e) => return Err(anyhow!(e)),
            }
        }

        let mut exclude_label_ids: HashSet<ObjectID> = HashSet::new();
        for label in excludes {
            match slebal_atad.get(Self::ser(label.id())?) {
                Ok(Some(bs)) => {
                    let object_ids: Vec<ObjectID> = Self::de(bs.to_vec())?;
                    exclude_label_ids.extend(object_ids.iter());
                }
                Ok(None) => {}
                Err(e) => return Err(anyhow!(e)),
            }
        }

        let results: Vec<ObjectID> = include_object_ids
            .par_iter()
            .filter_map(|id| match exclude_label_ids.contains(id) {
                true => None,
                false => Some(*id),
            })
            .collect();

        Ok(results)
    }
}
