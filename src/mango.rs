use anyhow::Result;
use std::path::{Path, PathBuf};

use super::bucket::Bucket;

#[derive(Clone, Debug)]
pub struct Mango {
    pub(crate) inner: sled::Db,
    path: PathBuf,
}

impl Mango {
    pub fn open(path: &Path) -> Result<Self> {
        path.to_path_buf().try_into()
    }

    pub fn get_bucket(&self, name: &str) -> Result<Bucket> {
        Bucket::open(name, self.clone())
    }

    pub fn empty_bucket(&self, name: &str) -> Result<()> {
        let b = Bucket::open(name, self.clone())?;
        b.empty()?;
        Ok(())
    }
}

impl TryFrom<PathBuf> for Mango {
    type Error = anyhow::Error;

    fn try_from(value: PathBuf) -> std::result::Result<Self, Self::Error> {
        let this = sled::Config::new()
            .path(value.clone())
            .compression_factor(16)
            .mode(sled::Mode::HighThroughput)
            .idgen_persist_interval(5000)
            .use_compression(true)
            .open()?;
        Ok(Self {
            inner: this,
            path: value,
        })
    }
}

impl<'a> From<&'a Mango> for PathBuf {
    fn from(val: &'a Mango) -> Self {
        val.path.clone()
    }
}
