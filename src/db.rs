use crate::internal::*;

#[derive(Clone, Debug)]
pub struct DB {
    pub(crate) inner: sled::Db,
    pub(crate) config: Config,
}

impl DB {
    pub fn new(config: Config) -> Result<Self> {
        let inner = sled::open(&config.db_path)?;
        log::trace!(target: "mango_chainsaw", "opening sled on {:?}", &config.db_path);
        Ok(Self { inner, config })
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
}
