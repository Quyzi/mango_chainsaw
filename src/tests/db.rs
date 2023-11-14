use bytes::Bytes;
use serde_json::json;
use storage::{StoreableItem, DefaultItem};

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
    let item = DefaultItem { inner: json!(thing) };

    let id = shard.insert(item.clone(), vec![])?;

    let got = match shard.get(id)? {
        Some(got) => got,
        None => return Err(storage::Error::Other(format!("oops"))),
    };

    assert_eq!(item, got);

    println!("{id}");

    Ok(())
}