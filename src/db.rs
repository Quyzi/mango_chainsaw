use std::path::PathBuf;
use crate::internal::*;

#[derive(Clone, Debug)]
pub struct DB {
    pub(crate) inner: sled::Db,
}

impl DB {
    pub fn new(path: &PathBuf) -> Result<Self> {
        let inner = sled::open(path)?;
        log::trace!(target: "mango_chainsaw", "opening sled on {:?}", path);
        Ok(Self { inner })
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

    pub async fn start_server(&self, address: String, port: u16) -> Result<()> {
        let server = ApiServer::new(self.clone(), address, port);

        server.run().await?;
        Ok(())
    }
}
