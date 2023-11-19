use crate::storage::{self, *};



use serde_derive::Deserialize;
use serde_derive::Serialize;
use serde_json::Value as JsonValue;

pub trait Store<'a>: Clone {
    type Error;
    type Db;
    type Config;
    type Item: StoreableItem<'a>;
    type Shard: StoreShard<'a>;

    /// Open the inner storage db
    fn open(config: Self::Config) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Get the inner storage db
    fn get_inner(&self) -> Self::Db;

    /// Get the inner storage db config
    fn get_config(&self) -> Self::Config;

    /// Open a named shard in the inner storage db
    fn open_shard(&self, name: &str) -> Result<Self::Shard, Self::Error>;

    /// Drop a named shard
    fn drop_shard(&self, shard: Self::Shard) -> Result<bool, Self::Error>;
}

/// Default Sled Storage
#[derive(Clone)]
pub struct DefaultStore {
    config: sled::Config,
    db: sled::Db,
}

impl<'a> Store<'a> for DefaultStore {
    type Error = storage::Error;
    type Config = sled::Config;
    type Db = sled::Db;
    type Item = DefaultItem;
    type Shard = DefaultStorageShard;

    fn open(config: Self::Config) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let db = config.open()?;
        Ok(Self { config, db })
    }

    fn get_inner(&self) -> Self::Db {
        self.db.clone()
    }

    fn get_config(&self) -> Self::Config {
        self.config.clone()
    }

    fn open_shard(&self, name: &str) -> Result<Self::Shard, Self::Error> {
        let shard = Self::Shard::new(self.clone(), name)?;
        Ok(shard)
    }

    fn drop_shard(&self, shard: Self::Shard) -> Result<bool, Self::Error> {
        shard.drop()
    }
}

/// Default Item
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct DefaultItem {
    pub inner: JsonValue,
}

impl Storeable for DefaultItem {}
