use anyhow::Result;
use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::namespace::Namespace;

#[cfg(test)]
use tempfile::TempDir;

#[derive(Clone)]
pub struct Db {
    #[allow(dead_code)]
    pub(crate) opened: u64,
    #[allow(dead_code)]
    pub(crate) path: PathBuf,
    pub(crate) inner: sled::Db,
}

impl Db {
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

    pub fn open_namespace(&self, name: &str) -> Result<Namespace> {
        Namespace::open_from_db(self.inner.clone(), name)
    }

    /// Get the next ID from sled monotonic counter
    pub(crate) fn next_id(&self) -> Result<u64> {
        Ok(self.inner.generate_id()?)
    }
}
