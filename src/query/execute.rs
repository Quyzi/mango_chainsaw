use bytes::Bytes;
use flexbuffers::FlexbufferSerializer;
use serde::{de::DeserializeOwned, Serialize};
use sled::transaction::TransactionalTree;

use crate::label::Label;

use super::error::TransactionError;

pub trait ExecuteTransaction<'a> {
    type Error: std::error::Error + From<TransactionError>;
    type Output;

    fn transaction_ser<T: Serialize>(item: T) -> Result<Bytes, Self::Error> {
        let mut s = FlexbufferSerializer::new();
        item.serialize(&mut s).map_err(|e| e.into())?;
        Ok(s.take_buffer().into())
    }

    fn transaction_de<T: DeserializeOwned>(bytes: Bytes) -> Result<T, Self::Error> {
        Ok(flexbuffers::from_slice(&bytes).map_err(|e| e.into())?)
    }

    fn ser_label(label: Label) -> Result<Bytes, Self::Error> {
        Self::transaction_ser(label.to_string_ltr())
    }

    fn ser_label_invert(label: Label) -> Result<Bytes, Self::Error> {
        Self::transaction_ser(label.to_string_rtl())
    }

    fn de_label(bytes: Bytes) -> Result<Label, Self::Error> {
        Self::transaction_de(bytes)
    }

    fn execute(
        &self,
        lbl: &'a TransactionalTree,
        ilbl: &'a TransactionalTree,
        obj: &'a TransactionalTree,
        objlbl: &'a TransactionalTree,
        objilbl: &'a TransactionalTree,
    ) -> Result<Self::Output, Self::Error>;
}
