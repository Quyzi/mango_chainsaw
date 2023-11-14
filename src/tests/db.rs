use super::item::new_thing;
use crate::storage::{self, DefaultStore, Store, StoreShard};

type Result<T> = std::result::Result<T, storage::Error>;

#[test]
fn test_db() -> Result<()> {
    let config = sled::Config::new()
        .temporary(true);
    let store = DefaultStore::open(config)?;
    let shard = store.open_shard("testing")?;

    let thing = new_thing()?;
    let item = thing.try_into()?;
    
    let id = shard.insert(item, vec![])?;


    Ok(())
}