use crate::{bucket::Bucket, object::ObjectID};
use anyhow::Result;
use bytes::Bytes;
use flexbuffers::FlexbufferSerializer;
use std::cell::RefCell;

#[derive(Clone)]
pub struct GetRequest {
    ids: RefCell<Vec<ObjectID>>,
}

impl GetRequest {
    pub fn new(ids: Vec<ObjectID>) -> Result<Self> {
        Ok(Self {
            ids: RefCell::new(ids),
        })
    }

    fn ser<T: serde::Serialize>(item: T) -> Result<Bytes> {
        let mut s = FlexbufferSerializer::new();
        item.serialize(&mut s)?;
        Ok(s.take_buffer().into())
    }

    pub fn execute(self, bucket: Bucket) -> Result<Vec<(ObjectID, Option<Vec<u8>>)>> {
        let ids = self.ids.take();

        let tree = bucket.t_objects.clone();

        let mut results = vec![];
        for id in ids {
            let key_bytes = Self::ser(id)?;
            match tree.get(&key_bytes) {
                Ok(Some(bytes)) => results.push((id, Some(bytes.to_vec()))),
                Ok(None) => results.push((id, None)),
                Err(e) => {
                    log::error!("error getting object with id {id}: {e}");
                    results.push((id, None))
                }
            }
        }

        Ok(results)
    }
}
