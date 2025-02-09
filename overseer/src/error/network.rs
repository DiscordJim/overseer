use thiserror::Error;

#[derive(Error, Debug)]
pub enum NetworkError {
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
    UnknownPacketSchema(u8)
}