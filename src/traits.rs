use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::path::PathBuf;
use bytes::Bytes;
use serde::Serialize;
use serde::Deserialize;
pub trait Db {
    type Namespace;
    type Error;

    fn open(path: PathBuf) -> Result<Self, Self::Error> where Self: Sized;

    fn list_namespaces(&self) -> Result<Vec<String>, Self::Error>;

    fn get_namespace(&self, name: &str) -> Result<Self::Namespace, Self::Error>;

    fn drop_namespace(&self, name: &str) -> Result<bool, Self::Error>;
}

pub trait Namespace {
    type Error;
    type Label;
    type Item;
    type Db;
    type Id;

    fn new(db: &Self::Db, name: &str) -> Self;
    
    fn put(&self, item: Self::Item, labels: Vec<Self::Label>) -> Result<Self::Id, Self::Error>;

    fn get(&self, id: Self::Id) -> Result<Option<Self::Item>, Self::Error>;

    fn delete(&self, id: Self::Id) -> Result<bool, Self::Error>;

    fn compare_swap(&self, id: Self::Id, item: Self::Item) -> Result<Option<Self::Item>, Self::Error>;
}
