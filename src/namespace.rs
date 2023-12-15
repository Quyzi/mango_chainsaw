use anyhow::Result;
use sled::Tree;

/// Separator character for tree names.
pub(crate) const SEPARATOR: &str = "\u{001F}";

#[derive(Clone, Debug)]
pub struct Namespace {
    pub name: String,

    #[allow(dead_code)]
    pub(crate) db: sled::Db,

    /// [Label ID] => [Label]
    pub(crate) labels: Tree,

    /// [Label content] => [Label ID]
    pub(crate) labels_inverse: Tree,

    /// [Object ID] => [Object Bytes]
    pub(crate) data: Tree,

    /// [Object ID] => [Vec<Label ID>]
    pub(crate) data_labels: Tree,

    /// [Label ID] => [Vec<Object ID>]
    pub(crate) data_labels_inverse: Tree,
}

impl Namespace {
    pub(crate) fn open_from_db(db: sled::Db, name: &str) -> Result<Self> {
        Ok(Self {
            name: name.to_string(),
            db: db.clone(),
            labels: db.open_tree(format!("{name}{SEPARATOR}labels"))?,
            labels_inverse: db.open_tree(format!("{name}{SEPARATOR}labels_inverse"))?,
            data: db.open_tree(format!("{name}{SEPARATOR}data"))?,
            data_labels: db.open_tree(format!("{name}{SEPARATOR}data_labels"))?,
            data_labels_inverse: db.open_tree(format!("{name}{SEPARATOR}data_labels_inverse"))?,
        })
    }
}
