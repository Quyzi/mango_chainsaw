use anyhow::anyhow;
use anyhow::Result;
use bytes::Bytes;
use flexbuffers::FlexbufferSerializer;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sled::transaction::ConflictableTransactionError;
use sled::transaction::UnabortableTransactionError;
use sled::Transactional;
use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::common::*;
use crate::{db::Db, namespace::Namespace};

#[derive(Clone, Default)]
pub struct InsertRequest {
    pub id: ObjectID,
    pub(crate) obj: Object,
    pub labels: HashSet<Label>,
}

impl InsertRequest {
    /// Create a new InsertRequest using the hash of the payload as the object ID
    pub fn new(payload: Bytes, labels: Vec<Label>) -> Self {
        Self {
            id: {
                let mut hasher = DefaultHasher::new();
                payload.hash(&mut hasher);
                hasher.finish()
            },
            obj: Arc::new(payload),
            labels: HashSet::from_iter(labels.iter().cloned()),
        }
    }

    /// Create a new InsertRequest using a monotonic counter to generate the object ID
    pub fn new_using_db(db: &Db, payload: Bytes, labels: Vec<Label>) -> Result<Self> {
        let mut this = Self::new(payload, labels);
        this.id = db.next_id()?;
        Ok(this)
    }

    /// Helper serialization fn to serialize a thing inside a transaction block
    pub(crate) fn ser<T: Serialize>(thing: T) -> Result<Vec<u8>, UnabortableTransactionError> {
        let mut s = FlexbufferSerializer::new();
        thing.serialize(&mut s).map_err(|e| {
            UnabortableTransactionError::Storage(sled::Error::Io(std::io::Error::other(e)))
        })?;
        Ok(s.take_buffer())
    }

    /// Helper deserialization fn to serialize a thing inside a transaction block
    pub(crate) fn de<T: DeserializeOwned>(
        bytes: Vec<u8>,
    ) -> Result<T, UnabortableTransactionError> {
        let this = flexbuffers::from_slice(&bytes).map_err(|e| {
            UnabortableTransactionError::Storage(sled::Error::Io(std::io::Error::other(e)))
        })?;
        Ok(this)
    }

    /// Execute this insert request on a Namespace
    pub fn execute(self, ns: &Namespace) -> Result<ObjectID> {
        let labels = &ns.labels;
        let slebal = &ns.labels_inverse;
        let data = &ns.data;
        let data_labels = &ns.data_labels;
        let slebal_atad = &ns.data_labels_inverse;

        (labels, slebal, data, data_labels, slebal_atad)
            .transaction(
                |(tx_labels, tx_slebal, tx_data, tx_data_labels, tx_slebal_atad)| {
                    let object_id_bytes = Self::ser(self.id)?;

                    // Insert the data
                    tx_data.insert(object_id_bytes.clone(), Self::ser(&*self.obj)?)?;
                    log::debug!(
                        target: "mango_chainsaw::insert::execute",
                        "inserted object with id {id}",
                        id = &self.id,
                    );

                    // Collect label ids
                    let mut label_ids = vec![];

                    // Insert the labels and labels_inverse values
                    for label in &self.labels {
                        let id = label.id();
                        let value = label.data.clone();
                        let key_bytes = Self::ser(id)?;
                        let struct_bytes = Self::ser(label)?;
                        let value_bytes = Self::ser(&value)?;
                        tx_labels.insert(key_bytes.clone(), struct_bytes)?;
                        tx_slebal.insert(value_bytes, key_bytes)?;
                        label_ids.push(id);
                        log::debug!(
                            target: "mango_chainsaw::insert::execute",
                            "inserted label with id {id}: {value}"
                        );
                    }

                    // Insert data_labels
                    tx_data_labels.insert(object_id_bytes.clone(), Self::ser(&label_ids)?)?;
                    log::debug!(
                        target: "mango_chainsaw::insert::execute",
                        "inserted data_labels for id {id}",
                        id = &self.id,
                    );

                    // Upsert data_labels_inverse
                    for id in label_ids {
                        let label_id_bytes = Self::ser(id)?;
                        match tx_slebal_atad.remove(label_id_bytes.clone())? {
                            Some(old) => {
                                let mut object_ids: Vec<ObjectID> = Self::de(old.to_vec())?;
                                object_ids.push(self.id);
                                tx_slebal_atad
                                    .insert(object_id_bytes.clone(), Self::ser(object_ids)?)?;
                                log::debug!(
                                    target: "mango_chainsaw::insert::execute",
                                    "upserted existing data_labels with id {id}",
                                )
                            }
                            None => {
                                tx_slebal_atad
                                    .insert(label_id_bytes.clone(), Self::ser(vec![&self.id])?)?;
                                log::debug!(
                                    target: "mango_chainsaw::insert::execute",
                                    "inserted new data_labels with id {id}",
                                )
                            }
                        }
                    }
                    Ok::<(), ConflictableTransactionError<String>>(())
                },
            )
            .map_err(|e| anyhow!("{}", e))?;
        Ok(self.id)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bytes::Bytes;
    use log::LevelFilter;
    use serde_json::json;
    use simplelog::{ColorChoice, CombinedLogger, Config, TermLogger, TerminalMode};

    use crate::db::Db;

    use super::{InsertRequest, Label};

    #[test]
    pub fn test_insert() -> Result<()> {
        CombinedLogger::init(vec![TermLogger::new(
            LevelFilter::Debug,
            Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        )])?;

        let db = Db::open_temp()?;
        let ns = db.open_namespace("test_insert")?;

        let data = json!({
            "thing": "longer",
            "numbers": [
                4, 2, 0, 6, 9,
            ],
            "True": false,
        });

        let payload = Bytes::from(data.to_string());
        let labels = vec![
            Label::new("mango_chainsaw.localhost/datatype=testdata"),
            Label::new("mango_chainsaw.localhost/uuid=meowmeow-meow-meow-meow-meowmeowmeow"),
        ];

        let req = InsertRequest::new_using_db(&db, payload, labels)?;
        req.execute(&ns)?;

        Ok(())
    }
}
