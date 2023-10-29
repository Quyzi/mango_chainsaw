use std::path::PathBuf;
use rayon::prelude::*;
use crate::{internal::*, api::{v1::ApiServerV1, v2::start_server}};

#[derive(Clone, Debug)]
pub struct DB {
    pub(crate) inner: sled::Db,
    pub(crate) path: PathBuf,
}

impl DB {
    pub fn new(path: &PathBuf) -> Result<Self> {
        let inner = sled::open(path)?;
        log::trace!(target: "mango_chainsaw", "opening sled on {:?}", path);
        Ok(Self { inner, path: path.to_path_buf() })
    }

    pub fn open_namespace(&self, name: &str) -> Result<Namespace> {
        log::trace!(target: "mango_chainsaw", "opening namespace {}", name);
        Namespace::new(self, name)
    }

    pub fn drop_namespace(&self, namespace: Namespace) -> Result<()> {
        log::warn!(target: "mango_chainsaw", "dropping namespace {}", namespace.name);
        namespace.drop(self)?;
        Ok(())
    }
    
    pub fn list_namespaces(&self) -> Result<Vec<String>> {
        let mut namespaces: Vec<String> = self.list_trees()?.par_iter().filter_map(|tree| {
            if tree.starts_with("__sled__") {
                None
            } else {
                let name = tree.to_owned();
                Some(name.trim_end_matches("_labels").trim_end_matches("_blobs").to_string())
            }
        }).collect();
        namespaces.sort(); 
        namespaces.dedup();
        Ok(namespaces)
    }

    pub fn list_trees(&self) -> Result<Vec<String>> {
        let mut trees: Vec<String> = self.inner.tree_names().par_iter().filter_map(|bytes| {
            match bincode::deserialize::<&str>(&bytes) {
                Ok(name) => Some(name.to_string()),
                Err(outer) => {
                    match std::str::from_utf8(&bytes) {
                        Ok(name) => Some(name.to_string()),
                        Err(e) => {
                            log::error!(target: "mango_chainsaw", "error deserializing tree name: outer={outer} inner={e}");
                            None
                        },
                    }
                },
            }
        }).collect();
        trees.sort();
        trees.dedup();
        Ok(trees)
    }

    pub async fn start_server(&self, address: String, port: u16) -> Result<()> {
        start_server((address, port), self.clone()).await?;
        Ok(())
    }

    #[deprecated]
    pub async fn start_server_v1(&self, address: String, port: u16) -> Result<()> {
        let server = ApiServerV1::new(self.clone(), address, port);

        server.run().await?;
        Ok(())
    }
}
