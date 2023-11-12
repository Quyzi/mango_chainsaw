use crate::storage::{self, Store};

pub trait StoreShard<'a> {
    type Error;
    type Store: storage::Store<'a>;
    type Item: storage::StoreableItem<'a>;
    type MetadataItem: storage::MetadataItem<'a>;
    type ObjectId;

    /// Create or Open a shard by name
    fn new(parent: Self::Store, name: &str) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Human friendly name for this shard
    fn name(&self) -> String;

    /// Drop this shard
    fn drop(self) -> Result<bool, Self::Error>;

    /// Insert an object into this shard
    fn insert(
        &self,
        item: Self::Item,
        metadata: Vec<Self::MetadataItem>,
    ) -> Result<Self::ObjectId, Self::Error>;

    /// Get an object from this shard by id
    fn get(&self, id: Self::ObjectId) -> Result<Option<Self::Item>, Self::Error>;

    /// Find objects in this shard with the given Metadata items
    fn find(
        &self,
        meta: Vec<Self::MetadataItem>,
    ) -> Result<Option<Vec<Self::ObjectId>>, Self::Error>;
}

#[derive(Clone)]
pub struct DefaultStorageShard {
    name: String,
    parent: storage::DefaultStore,

    objects: sled::Tree,
    labels: sled::Tree,
    metadata: sled::Tree,
}

impl<'a> StoreShard<'a> for DefaultStorageShard {
    type Error = String;
    type Store = storage::DefaultStore;
    type Item = storage::DefaultItem;
    type MetadataItem = storage::DefaultMetadataItem;
    type ObjectId = u64;

    fn new(parent: storage::DefaultStore, name: &str) -> Result<Self, String>
    where
        Self: Sized,
    {
        let objects = parent
            .get_inner()
            .open_tree(format!("{}_objects", &name))
            .map_err(|e| format!("{e}"))?;
        let labels = parent
            .get_inner()
            .open_tree(format!("{}_labels", &name))
            .map_err(|e| format!("{e}"))?;
        let metadata = parent
            .get_inner()
            .open_tree(format!("{}_metadata", &name))
            .map_err(|e| format!("{e}"))?;

        Ok(Self {
            name: name.to_string(),
            parent,
            objects,
            labels,
            metadata,
        })
    }

    fn name(&self) -> String {
        self.name.to_string()
    }

    fn insert(
        &self,
        _item: Self::Item,
        _metadata: Vec<Self::MetadataItem>,
    ) -> Result<Self::ObjectId, Self::Error> {
        todo!()
    }

    fn drop(self) -> Result<bool, Self::Error> {
        todo!()
    }

    fn get(&self, _id: Self::ObjectId) -> Result<Option<Self::Item>, Self::Error> {
        todo!()
    }

    fn find(
        &self,
        _meta: Vec<Self::MetadataItem>,
    ) -> Result<Option<Vec<Self::ObjectId>>, Self::Error> {
        todo!()
    }
}
