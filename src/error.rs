use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {

    #[error("io error {0}")]
    IoError(#[from] std::io::Error),

    #[error("sled error {0}")]
    SledError(#[from] sled::Error),

    #[error("sled transaction error {0}")]
    TransactionError(#[from] sled::transaction::TransactionError),

    #[error("bincode error {0}")]
    BincodeError(#[from] Box<bincode::ErrorKind>),
    
    #[error("other error {0}")]
    Other(String),
}