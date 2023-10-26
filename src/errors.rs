use thiserror::Error;

pub type Result<T> = std::result::Result<T, MangoChainsawError>;

#[derive(Error, Debug)]
pub enum MangoChainsawError {
    #[error("Sled Error: {0}")]
    Sled(#[from] sled::Error),

    #[error("Bincode Error: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("IO Error: {0}")]
    IO(#[from] std::io::Error),
}
