use thiserror::Error;

#[derive(Error, Debug)]
pub enum WalError {
    #[error("Database connection error: {0}")]
    ConnectionError(#[from] tokio_postgres::Error),

    #[error("Replication slot error: {0}")]
    ReplicationSlotError(String),

    #[error("Publication error: {0}")]
    PublicationError(String),

    #[error("WAL decoding error: {0}")]
    DecodingError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Handler error: {0}")]
    HandlerError(String),

    #[error("Unexpected message type: {0}")]
    UnexpectedMessage(String),
}

pub type WalResult<T> = Result<T, WalError>;