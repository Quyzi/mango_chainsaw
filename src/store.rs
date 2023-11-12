use crate::storage::*;
use serde_derive::{Deserialize, Serialize};
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
    fn drop_shard(&self, name: &str) -> Result<bool, Self::Error>;
}

/// Default Sled Storage
#[derive(Clone)]
pub struct DefaultStore {
    config: sled::Config,
    db: sled::Db,
}

impl<'a> Store<'a> for DefaultStore {
    type Error = String;
    type Config = sled::Config;
    type Db = sled::Db;
    type Item = DefaultItem;
    type Shard = DefaultStorageShard;

    fn open(config: Self::Config) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let db = config.open().map_err(|e| format!("{e}"))?;
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

    fn drop_shard(&self, _name: &str) -> Result<bool, Self::Error> {
        todo!()
    }
}

/// Default Item
#[derive(Clone, Serialize, Deserialize)]
pub struct DefaultItem {
    #[serde(flatten)]
    pub inner: JsonValue,
}
impl Storeable for DefaultItem {}
