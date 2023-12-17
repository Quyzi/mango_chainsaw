use anyhow::Result;
use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::namespace::Namespace;

#[cfg(test)]
use tempfile::TempDir;

/// The MangoChainsaw DB
#[derive(Clone)]
pub struct Db {
    pub(crate) opened: u64,
    pub(crate) path: PathBuf,
    pub(crate) inner: sled::Db,
}

impl Db {
    /// Open a MangoChainsaw db at a given Path
    pub fn open(path: &Path) -> Result<Self> {
        let now = {
            let now = SystemTime::now();
            match now.duration_since(UNIX_EPOCH) {
                Ok(now) => now.as_secs(),
                Err(e) => {
                    log::error!("error getting current time: {e}");
                    0
                }
            }
        };

        Ok(Self {
            inner: sled::open(path)?,
            path: path.into(),
            opened: now,
        })
    }

    #[cfg(test)]
    #[allow(dead_code)]
    /// Open a MangoChainsaw db in a tempdir
    pub(crate) fn open_temp() -> Result<Self> {
        let temp = TempDir::new()?;
        let now = {
            let now = SystemTime::now();
            match now.duration_since(UNIX_EPOCH) {
                Ok(now) => now.as_secs(),
                Err(e) => {
                    log::error!("error getting current time: {e}");
                    0
                }
            }
        };
        Ok(Self {
            opened: now,
            path: temp.path().into(),
            inner: sled::open(temp.path())?,
        })
    }

    /// Get the timestamp the db was opened
    pub fn opened(&self) -> u64 {
        self.opened
    }

    /// Get the path of the db
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Open a Namespace by name
    pub fn open_namespace(&self, name: &str) -> Result<Namespace> {
        Namespace::open_from_db(self.inner.clone(), name)
    }

    /// Get the next ID from sled monotonic counter
    pub(crate) fn next_id(&self) -> Result<u64> {
        Ok(self.inner.generate_id()?)
    }
}
