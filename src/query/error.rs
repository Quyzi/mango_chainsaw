use thiserror::Error;

#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("transaction already executed")]
    AlreadyExecuted,

    #[error("serialization error: {0}")]
    SerializationError(#[from] flexbuffers::SerializationError),

    #[error("deserialization error: {0}")]
    DeserializationError(#[from] flexbuffers::DeserializationError),

    #[error("sled error: {0}")]
    SledError(#[from] sled::Error),

    #[error("sled transaction error: {0}")]
    SledTxError(#[from] sled::transaction::TransactionError),

    #[error("sled transaction error: {0}")]
    SledUnabortableError(#[from] sled::transaction::UnabortableTransactionError),

    #[error("anyhow error: {0}")]
    Anyhow(#[from] anyhow::Error),
}

impl From<TransactionError> for sled::transaction::UnabortableTransactionError {
    fn from(value: TransactionError) -> Self {
        sled::transaction::UnabortableTransactionError::Storage(sled::Error::Unsupported(
            value.to_string(),
        ))
    }
}
