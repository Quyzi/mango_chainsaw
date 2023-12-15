use anyhow::Result;
use anyhow::anyhow;
use bytes::Bytes;
use flexbuffers::FlexbufferSerializer;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use sled::transaction::UnabortableTransactionError;
use std::{collections::{HashSet, hash_map::DefaultHasher}, sync::Arc, hash::{Hasher, Hash}};
use sled::Transactional;
use sled::transaction::ConflictableTransactionError;

use crate::{db::Db, namespace::Namespace};

#[derive(Clone, Debug, Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Label {
    pub data: String,
}

impl Label {
    pub fn id(&self) -> LabelID {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

pub type ObjectID = u64;
pub type Object = Arc<Bytes>;
pub type LabelID = u64;


#[derive(Clone)]
pub struct InsertRequest {
    pub id: ObjectID,
    pub(crate) obj: Object,
    pub labels: HashSet<Label>,
}

impl Default for InsertRequest {
    fn default() -> Self {
        Self {
            id: 0,
            obj: Default::default(),
            labels: Default::default(),
        }
    }
}

impl InsertRequest {
    /// Create a new InsertRequest using the hash of the payload as the object ID
    pub fn new(payload: Bytes, labels: HashSet<Label>) -> Self {
        let mut this = Self::default();
        this.id = {
            let mut hasher = DefaultHasher::new();
            payload.hash(&mut hasher);
            hasher.finish()
        };
        this.obj = Arc::new(payload);
        this.labels = labels;
        this
    }

    /// Create a new InsertRequest using a monotonic counter to generate the object ID
    pub fn new_using_db(db: &Db, payload: Bytes, labels: HashSet<Label>) -> Result<Self> {
        let mut this = Self::new(payload, labels);
        this.id = db.next_id()?;
        Ok(this)
    }

    /// Helper serialization fn to serialize a thing inside a transaction block
    pub(crate) fn ser<T: Serialize>(thing: T) -> Result<Vec<u8>, UnabortableTransactionError> {
        let mut s = FlexbufferSerializer::new();
        thing.serialize(&mut s)
            .map_err(|e| UnabortableTransactionError::Storage(sled::Error::Io(std::io::Error::other(e))))?;
        Ok(s.take_buffer())
    }

    /// Helper deserialization fn to serialize a thing inside a transaction block
    pub(crate) fn de<T: DeserializeOwned>(bytes: Vec<u8>) -> Result<T, UnabortableTransactionError> {
        let this = flexbuffers::from_slice(&bytes)
            .map_err(|e| UnabortableTransactionError::Storage(sled::Error::Io(std::io::Error::other(e))))?;
        Ok(this)
    }

    /// Execute this insert request on a Namespace
    pub fn execute(self, ns: Namespace) -> Result<()> {
        let labels = &ns.labels;
        let slebal = &ns.labels_inverse;
        let data = &ns.data;
        let data_labels = &ns.data_labels;
        let slebal_atad = &ns.data_labels_inverse;

        (labels, slebal, data, data_labels, slebal_atad)
            .transaction(|(tx_labels, tx_slebal, tx_data, tx_data_labels, tx_slebal_atad)| {
                let object_id_bytes = Self::ser(&self.id)?;

                // Insert the data
                tx_data.insert(object_id_bytes.clone(), Self::ser(&*self.obj)?)?;

                // Collect label ids 
                let mut label_ids = vec![];

                // Insert the labels and labels_inverse values
                for label in &self.labels {
                    let id = label.id();
                    let value = label.data.clone();
                    let key_bytes = Self::ser(&id)?;
                    let struct_bytes = Self::ser(&label)?;
                    let value_bytes = Self::ser(&value)?;
                    tx_labels.insert(key_bytes.clone(), struct_bytes)?;
                    tx_slebal.insert(value_bytes, key_bytes)?;
                    label_ids.push(id);
                }

                // Insert data_labels
                tx_data_labels.insert(object_id_bytes.clone(), Self::ser(&label_ids)?)?;
                
                // Upsert data_labels_inverse
                for id in label_ids {
                    let label_id_bytes = Self::ser(&id)?;
                    match tx_slebal_atad.remove(label_id_bytes.clone())? {
                        Some(old) => {
                            let mut object_ids: Vec<ObjectID> = Self::de(old.to_vec())?;
                            object_ids.push(self.id);
                            tx_slebal_atad.insert(object_id_bytes.clone(), Self::ser(object_ids)?)?;
                            ()
                        },
                        None => {
                            tx_slebal_atad.insert(label_id_bytes.clone(), Self::ser(vec![&self.id])?)?;
                            ()
                        },
                    }
                }
                Ok::<(), ConflictableTransactionError<String>>(())
            }).map_err(|e| anyhow!("{}", e))?;
        Ok(())
    }
}
