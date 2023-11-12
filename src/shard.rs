use crate::storage::{self, Store};
use rayon::prelude::*;
use storage::{Metadata, StoreableItem, MetadataItem};
use sled::Transactional;

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
        metadata_items: Vec<Self::MetadataItem>,
    ) -> Result<Self::ObjectId, Self::Error>;

    /// Get an object from this shard by id
    fn get(&self, id: Self::ObjectId) -> Result<Option<Self::Item>, Self::Error>;

    /// Get many objects from this shard by id
    fn get_many(&self, ids: Vec<Self::ObjectId>) -> Result<Vec<(Self::ObjectId, Option<Self::Item>)>, Self::Error>;

    /// Find objects in this shard with the given Metadata items
    fn find(
        &self,
        meta: Vec<Self::MetadataItem>,
    ) -> Result<Option<Vec<Self::ObjectId>>, Self::Error>;

    /// Delete an object from this shard by id
    fn delete(&self, id: Self::ObjectId) -> Result<Option<Self::Item>, Self::Error>;

    /// Delete many objects from this shard by id
    fn delete_many(&self, ids: Vec<Self::ObjectId>) -> Result<Vec<(Self::ObjectId, Option<Self::Item>)>, Self::Error>;

    /// Compare and swap. 
    /// Capable of unique creation, conditional modification, or deletion. 
    /// 
    /// If old is None, this will only set the value if it doesn’t exist yet. 
    /// 
    /// If new is None, will delete the value if old is correct. 
    /// 
    /// If both old and new are Some, will modify the value if old is correct.
    fn compare_swap_object(&self, id: Self::ObjectId, old: Option<Self::Item>, new: Option<Self::Item>) -> Result<(), Self::Error>;

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
    type Error = storage::Error;
    type Store = storage::DefaultStore;
    type Item = storage::DefaultItem;
    type MetadataItem = storage::DefaultMetadataItem;
    type ObjectId = u64;

    fn new(parent: storage::DefaultStore, name: &str) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let objects = parent
            .get_inner()
            .open_tree(format!("{}_objects", &name))?;
        let labels = parent
            .get_inner()
            .open_tree(format!("{}_labels", &name))?;
        let metadata = parent
            .get_inner()
            .open_tree(format!("{}_metadata", &name))?;

        Ok(Self {
            name: name.to_string(),
            parent,
            objects,
            labels,
            metadata,
        })
    }

    fn drop(self) -> Result<bool, Self::Error> {
        let db = self.parent.get_inner();
        if vec![
            db.drop_tree(format!("{}_objects", &self.name)),
            db.drop_tree(format!("{}_labels", &self.name)),
            db.drop_tree(format!("{}_metadata", &self.name)),
        ].into_par_iter().all(|r| r.is_ok()) {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn name(&self) -> String {
        self.name.to_string()
    }

    fn insert(
        &self,
        item: Self::Item,
        metadata_items: Vec<Self::MetadataItem>,
    ) -> Result<Self::ObjectId, Self::Error> {
        let id = self.parent.get_inner().generate_id()?;
        let metadata = storage::DefaultMetadata::new(id, metadata_items.clone())?;
        
        let obj = (metadata.db_key()?, item.to_vec()?);
        let meta = (metadata.db_key()?, metadata.to_bytes()?);
        let labels: Vec<(Vec<u8>, Vec<u8>)> = metadata_items.into_par_iter().filter_map(|item| {
            let key = match item.key_bytes() {
                Ok(bs) => bs,
                Err(_e) => return None
            };
            let value = match item.val_bytes() {
                Ok(bs) => bs,
                Err(_e) => return None,
            };
            Some((key, value))
        }).collect();

        (&self.objects, &self.labels, &self.metadata).transaction(|(tx_obj, tx_label, tx_metadata)| {
            let obj = obj.clone();
            tx_obj.insert(obj.0, obj.1)?;

            let labels = labels.clone();
            for label in labels {
                tx_label.insert(label.0, label.1)?;
            }

            let meta = meta.clone();
            tx_metadata.insert(meta.0, meta.1)?;

            Ok(())
        })?;
        
        Ok(id)
    }

    fn get(&self, id: Self::ObjectId) -> Result<Option<Self::Item>, Self::Error> {
        todo!()
    }

    fn get_many(&self, _ids: Vec<Self::ObjectId>) -> Result<Vec<(Self::ObjectId, Option<Self::Item>)>, Self::Error> {
        todo!()
    }

    fn find(
        &self,
        _meta: Vec<Self::MetadataItem>,
    ) -> Result<Option<Vec<Self::ObjectId>>, Self::Error> {
        todo!()
    }

    fn delete(&self, id: Self::ObjectId) -> Result<Option<Self::Item>, Self::Error> {
        todo!()
    }

    fn delete_many(&self, ids: Vec<Self::ObjectId>) -> Result<Vec<(Self::ObjectId, Option<Self::Item>)>, Self::Error> {
        todo!()
    }

    fn compare_swap_object(&self, id: Self::ObjectId, old: Option<Self::Item>, new: Option<Self::Item>) -> Result<(), Self::Error> {
        todo!()
    }

    
}
