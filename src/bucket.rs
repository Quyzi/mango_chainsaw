use std::cell::RefCell;

use super::mango::Mango;
use anyhow::Result;
use sled::Tree;

pub const SEPARATOR: &str = "\u{001F}";

#[derive(Clone, Debug)]
pub struct Bucket {
    parent: Mango,
    name: String,
    is_ok: RefCell<bool>,

    /// Key = ([lhs][SEPARATOR][rhs]), Value = Label
    ///
    /// Stores Labels and their objects in lhs=rhs form.
    pub(crate) t_labels: Tree,

    /// Key = ([rhs][SEPARATOR][lhs]), Value = Label
    ///
    /// Stores labels and their objects in rhs=lhs form.
    pub(crate) t_labels_invert: Tree,

    /// Key = ObjectID, Value = Object
    ///
    /// Stores the raw objects as Bytes
    pub(crate) t_objects: Tree,

    /// Key = ObjectID, Value = Vec<Label>
    ///
    /// Stores A list of labels describing each object
    pub(crate) t_objects_labels: Tree,

    /// Key = Label = Vec<ObjectID>
    ///
    /// Stores a list of Objects described by a specific label
    pub(crate) t_labels_objects: Tree,
}

impl Bucket {
    pub(crate) fn open(name: &str, parent: Mango) -> Result<Self> {
        let db = parent.inner.clone();
        Ok(Self {
            parent: parent.clone(),
            name: name.to_string(),
            is_ok: RefCell::new(true),
            t_labels: db.open_tree(format!("{name}{SEPARATOR}labels"))?,
            t_labels_invert: db.open_tree(format!("{name}{SEPARATOR}ilabels"))?,
            t_objects: db.open_tree(format!("{name}{SEPARATOR}objects"))?,
            t_objects_labels: db.open_tree(format!("{name}{SEPARATOR}objectlabels"))?,
            t_labels_objects: db.open_tree(format!("{name}{SEPARATOR}objectilabels"))?,
        })
    }

    pub fn check(&self) -> Result<bool> {
        let ok = self.is_ok.try_borrow()?;
        Ok(*ok)
    }

    pub fn drop(&self) -> Result<()> {
        let name = &self.name;
        let db = self.parent.inner.clone();
        db.drop_tree(format!("{name}{SEPARATOR}labels"))?;
        db.drop_tree(format!("{name}{SEPARATOR}ilabels"))?;
        db.drop_tree(format!("{name}{SEPARATOR}objects"))?;
        db.drop_tree(format!("{name}{SEPARATOR}objectlabels"))?;
        db.drop_tree(format!("{name}{SEPARATOR}objectilabels"))?;

        let mut is_ok = self.is_ok.try_borrow_mut()?;
        *is_ok = false;

        Ok(())
    }
}
