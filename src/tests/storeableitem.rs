use std::{time::{SystemTime, UNIX_EPOCH}, collections::hash_map::DefaultHasher, hash::{Hasher, Hash}};

use bytes::Bytes;
use serde_derive::{Serialize, Deserialize};
use crate::storeableitem::*;

pub type Error = String;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Debug)]
pub struct TestyThing {
    pub b: bool,
    pub s: String,
    pub ss: Vec<String>,
    pub n: u128,
    pub nn: Vec<u128>,
}

fn new_thing() -> Result<TestyThing> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("{e}"))?
        .as_millis();

    let mut nums: Vec<u128> = vec![];
    let b = now % 409 == 0;
    let s = format!("{now}");
    let ss: Vec<String> = (0..now%17).enumerate().map(|(_, n)| {nums.push(n); format!("{n}")}).collect();
    let n = now % 239;
    let nn = nums;
    
    Ok(TestyThing {
        b, s, ss, n, nn
    })
}

#[test]
fn test_storeable_item() -> Result<()> {
    let this = new_thing()?;
    let thishash = {
        let mut hasher = DefaultHasher::new();
        this.hash(&mut hasher);
        hasher.finish()
    };

    let bytes = this.to_vec()?;
    let that = TestyThing::from_vec(&Bytes::from(bytes))?;
    let thathash = {
        let mut hasher = DefaultHasher::new();
        that.hash(&mut hasher);
        hasher.finish()
    };

    let hb = bincode::serialize(&thathash).map_err(|e| format!("{e}"))?;

    assert_eq!(this, that);
    assert_eq!(thishash, thathash);
    assert_eq!(this.key(), Ok(hb.clone()));
    assert_eq!(that.key(), Ok(hb.clone()));
    println!("{thishash} :: {thathash}");
    Ok(())
}