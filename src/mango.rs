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

    pub fn get_namespace(&self, name: &str) -> Result<Bucket> {
        Bucket::open(name, self.clone())
    }
}

impl TryFrom<PathBuf> for Mango {
    type Error = anyhow::Error;

    fn try_from(value: PathBuf) -> std::prelude::v1::Result<Self, Self::Error> {
        let this = sled::open(value.clone())?;
        Ok(Self {
            inner: this,
            path: value,
        })
    }
}

impl<'a> TryFrom<&'a Path> for Mango {
    type Error = anyhow::Error;

    fn try_from(value: &'a Path) -> std::prelude::v1::Result<Self, Self::Error> {
        let this = sled::open(value)?;
        Ok(Self {
            inner: this,
            path: value.to_path_buf(),
        })
    }
}

impl<'a> From<&'a Mango> for PathBuf {
    fn from(val: &'a Mango) -> Self {
        val.path.clone()
    }
}
