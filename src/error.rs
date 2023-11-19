use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("io error {0}")]
    IoError(#[from] std::io::Error),

    #[error("sled error {0}")]
    SledError(#[from] sled::Error),

    #[error("sled transaction error {0}")]
    TransactionError(#[from] sled::transaction::TransactionError),

    #[error("flexbuffer serialization error {0}")]
    FlexSerError(#[from] flexbuffers::SerializationError),

    #[error("flexbuffer deserialization error {0}")]
    FlexDeError(#[from] flexbuffers::DeserializationError),

    #[error("flexbuffer reader error {0}")]
    FlexReaderError(#[from] flexbuffers::ReaderError),

    #[error("other error {0}")]
    Other(String),
}
