use std::array::TryFromSliceError;

use thiserror::Error;



#[derive(Error, Debug)]
pub enum NetworkError {
    // #[error("Try from slice fail")]
    // FailedSliceConversion(#[from] TryFromSliceError),
    #[error("Illegal read.")]
    IllegalRead,
    #[error("This page is freed and cannot be freely acquired.")]
    PageFreedError,
    #[error("PAge was out of bounds")]
    PageOutOfBounds,
    #[error("Failed to connect to address.")]
    SocketBindFailure(tokio::io::Error),
    #[error("Unrecognized packet discriminator")]
    UnrecognizedPacketTypeDiscriminator(u8),
    #[error("Error reading bytes.")]
    IoError(#[from] tokio::io::Error),
    #[error("Unrecognized value type discriminator")]
    UnrecognizedValueTypeDiscriminator(u8),
    #[error("Failed to read key")]
    FailedToReadKey,
    #[error("Failed to read value")]
    FailedToReadValue,
    #[error("Invalid watcher activity")]
    WatcherActivityDecodeError,
    #[error("Invalid watcher behaviour")]
    WatcherBehaviourDecodeError,
    #[error("Could not decode option")]
    ErrorDecodingOption,
    #[error("Could not decode boolean")]
    ErrorDecodingBoolean,
    #[error("Unknown packet schema")]
    UnknownPacketSchema(u8),
    #[error("Storage error")]
    StorageError(#[from] sqlx::error::Error),
    #[error("Could not conver from slice")]
    TrySliceError(#[from] std::array::TryFromSliceError),
    #[error("Could not convert from slice")]
    ParseUtf8Error(#[from] std::str::Utf8Error),
    #[error("Error converting to socket")]
    SocketError,
    #[error("Failed to connect to the socket")]
    FailedToConnectToSocket,
    #[error("Wrong response from server")]
    WrongResponseFromServer
}