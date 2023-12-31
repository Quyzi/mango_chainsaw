use crate::{label::Label, object::ObjectID};
use anyhow::Result;

use sled::transaction::UnabortableTransactionError;
use std::{cell::RefCell, collections::HashSet};

use super::execute::ExecuteTransaction;

#[derive(Clone, Debug)]
pub enum LabelGroup {
    Include(Vec<Label>),
    Exclude(Vec<Label>),
}

#[derive(Clone, Debug)]
pub struct FindRequest {
    groups: RefCell<Vec<LabelGroup>>,
}

impl FindRequest {
    pub fn new() -> Result<Self> {
        Ok(Self {
            groups: RefCell::new(vec![]),
        })
    }

    pub fn add_include_group(&self, labels: Vec<Label>) -> Result<()> {
        let mut label_groups = self.groups.try_borrow_mut()?;
        label_groups.push(LabelGroup::Include(labels));
        Ok(())
    }

    pub fn add_exclude_group(&self, labels: Vec<Label>) -> Result<()> {
        let mut label_groups = self.groups.try_borrow_mut()?;
        label_groups.push(LabelGroup::Exclude(labels));
        Ok(())
    }
}

impl ExecuteTransaction for FindRequest {
    type Error = UnabortableTransactionError;
    type Output = Vec<(ObjectID, Vec<Label>)>;

    fn execute(
        &self,
        _lbl: &sled::transaction::TransactionalTree,
        _ilbl: &sled::transaction::TransactionalTree,
        _obj: &sled::transaction::TransactionalTree,
        objlbl: &sled::transaction::TransactionalTree,
        objilbl: &sled::transaction::TransactionalTree,
    ) -> std::prelude::v1::Result<Self::Output, Self::Error> {
        let groups = self
            .groups
            .try_borrow()
            .map_err(|e| {
                sled::transaction::UnabortableTransactionError::Storage(sled::Error::Unsupported(
                    e.to_string(),
                ))
            })?
            .clone();

        let mut group_results = vec![];
        for group in groups {
            let (labels, include) = match group.clone() {
                LabelGroup::Include(labels) => (labels, true),
                LabelGroup::Exclude(labels) => (labels, false),
            };

            let mut objects: HashSet<ObjectID> = HashSet::new();
            for label in labels {
                let key_bytes = Self::ser_label(label.clone())?;
                match objilbl.get(&key_bytes) {
                    Ok(Some(bytes)) => {
                        let ids: Vec<ObjectID> = Self::transaction_de(bytes.to_vec().into())?;
                        objects.extend(ids);
                    }
                    Ok(None) => (),
                    Err(e) => {
                        log::error!(
                            "Error in Find request for label {}: {e}",
                            label.to_string_ltr()
                        );
                    }
                }
            }
            group_results.push((group, objects, include));
        }

        let objects = group_results
            .into_iter()
            .fold(HashSet::new(), |mut acc, item| {
                let (_group, objects, include) = item;
                let objects: HashSet<ObjectID> = HashSet::from_iter(objects);
                if include {
                    acc.extend(objects)
                } else {
                    acc.retain(|&id| !objects.contains(&id))
                }
                acc
            });

        let mut results = vec![];
        for id in objects {
            // Get all of the labels for this object
            let key_bytes = Self::transaction_ser(id)?;
            match objlbl.get(&key_bytes) {
                Ok(Some(bytes)) => {
                    let labels: Vec<Label> = Self::transaction_de(bytes.to_vec().into())?;
                    results.push((id, labels));
                }
                Ok(None) => results.push((id, vec![])),
                Err(e) => log::error!("Error in find request for object id {id}: {e}"),
            }
        }

        Ok(results)
    }
}
