use crate::item::*;
use crate::storage;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    hash::Hash,
    time::{SystemTime, UNIX_EPOCH},
};


pub type Result<T> = std::result::Result<T, storage::Error>;

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Debug)]
pub struct TestyThing {
    pub b: bool,
    pub s: String,
    pub ss: Vec<String>,
    pub n: u64,
    pub nn: Vec<u64>,
}
impl Storeable for TestyThing {}

pub(super) fn new_thing() -> Result<TestyThing> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| storage::Error::Other(format!("{e}")))?
        .as_secs();

    let mut nums: Vec<u64> = vec![];
    let b = now % 409 == 0;
    let s = format!("{now}");
    let ss: Vec<String> = (0..now % 17 + 2)
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
    let that = TestyThing::from_bytes(&Bytes::from(bytes))?;
    let thathash = that.try_hash()?;

    assert_eq!(this, that);
    assert_eq!(thishash, thathash);
    assert_eq!(this.hashkey()?, that.hashkey()?);
    println!("{thishash} :: {thathash}");
    Ok(())
}

#[test]
fn test_flexbuf() -> Result<()> {
    let mut ser = flexbuffers::FlexbufferSerializer::new();
    let this = new_thing()?;
    this.serialize(&mut ser)?;
    let this_bytes = ser.take_buffer();
    let that = flexbuffers::from_slice(&this_bytes)?;

    assert_eq!(this, that);

    Ok(())
}
