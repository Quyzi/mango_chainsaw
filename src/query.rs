use crate::{common::*, namespace::Namespace};
use anyhow::{anyhow, Result};
use flexbuffers::FlexbufferSerializer;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::{de::DeserializeOwned, Serialize};
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
            QueryError::AlreadyExecuted => write!(f, "Query Already Executed"),
            _ => write!(f, "Undefined"),
        }
    }
}

pub struct QueryRequest {
    pub include_labels: RefCell<HashSet<Label>>,
    pub exclude_labels: RefCell<HashSet<Label>>,
    pub results: RefCell<Option<HashSet<ObjectID>>>,
    executed: RefCell<bool>,
}

impl Default for QueryRequest {
    fn default() -> Self {
        Self {
            include_labels: RefCell::new(HashSet::new()),
            exclude_labels: RefCell::new(HashSet::new()),
            results: RefCell::new(None),
            executed: RefCell::new(false),
        }
    }
}

impl QueryRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_executed(&self) -> Result<bool> {
        Ok(*self.executed.try_borrow()?)
    }

    pub fn include(&self, label: Label) -> Result<()> {
        if self.is_executed()? {
            return Err(anyhow!(QueryError::AlreadyExecuted));
        }
        let mut labels = self.include_labels.try_borrow_mut()?;
        labels.insert(label);
        Ok(())
    }

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

    pub async fn execute(&self, ns: Namespace) -> Result<Vec<ObjectID>> {
        if self.is_executed()? {
            return Err(anyhow!(QueryError::AlreadyExecuted));
        }

        let slebal_atad = &ns.data_labels_inverse;

        {
            let mut executed = self.executed.try_borrow_mut()?;
            *executed = true;
        }

        let includes = self.include_labels.take();
        let excludes = self.exclude_labels.take();

        let mut include_label_ids: HashSet<ObjectID> = HashSet::new();
        for label in includes {
            match slebal_atad.get(Self::ser(label.id())?) {
                Ok(Some(bs)) => {
                    let object_ids: Vec<ObjectID> = Self::de(bs.to_vec())?;
                    include_label_ids.extend(object_ids.iter());
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

        let results: Vec<ObjectID> = include_label_ids
            .par_iter()
            .filter_map(|id| match exclude_label_ids.contains(id) {
                true => None,
                false => Some(*id),
            })
            .collect();

        Ok(results)
    }
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use super::QueryRequest;
    use crate::common::Label;
    use anyhow::Result;
    use std::collections::HashSet;

    #[test]
    fn test_query() -> Result<()> {
        let this = QueryRequest::new();
        this.include(Label::new("mango_chainsaw.localhost/testdata=true"))?;
        this.exclude(Label::new("mango_chainsaw.localhost/production=false"))?;

        let includes = {
            let mut hs = HashSet::new();
            hs.insert(Label::new("mango_chainsaw.localhost/testdata=true"));
            hs
        };
        assert_eq!(this.include_labels.take(), includes);

        let excludes = {
            let mut hs = HashSet::new();
            hs.insert(Label::new("mango_chainsaw.localhost/production=false"));
            hs
        };
        assert_eq!(this.exclude_labels.take(), excludes);

        Ok(())
    }
}
