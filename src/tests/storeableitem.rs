use std::{
    hash::{Hash, Hasher},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::item::*;
use crate::storage;
use bytes::Bytes;
use serde_derive::{Deserialize, Serialize};

pub type Error = String;
pub type Result<T> = std::result::Result<T, storage::Error>;

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Debug)]
pub struct TestyThing {
    pub b: bool,
    pub s: String,
    pub ss: Vec<String>,
    pub n: u128,
    pub nn: Vec<u128>,
}
impl Storeable for TestyThing {}

fn new_thing() -> Result<TestyThing> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| storage::Error::Other(format!("{e}")))?
        .as_millis();

    let mut nums: Vec<u128> = vec![];
    let b = now % 409 == 0;
    let s = format!("{now}");
    let ss: Vec<String> = (0..now % 17)
        .enumerate()
        .map(|(_, n)| {
            nums.push(n);
            format!("{n}")
        })
        .collect();
    let n = now % 239;
    let nn = nums;

    Ok(TestyThing { b, s, ss, n, nn })
}

#[test]
fn test_storeable_item() -> Result<()> {
    let this = new_thing()?;
    let thishash = this.try_hash()?;

    let bytes = this.to_vec()?;
    let that = TestyThing::from_vec(&Bytes::from(bytes))?;
    let thathash = that.try_hash()?;

    assert_eq!(this, that);
    assert_eq!(thishash, thathash);
    assert_eq!(this.hashkey()?, that.hashkey()?);
    println!("{thishash} :: {thathash}");
    Ok(())
}
