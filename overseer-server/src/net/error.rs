use thiserror::Error;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Failed to connect to address.")]
    SocketBindFailure(#[from] tokio::io::Error),
    #[error("Unrecognized packet discriminator")]
    UnrecognizedPacketTypeDiscriminator(u8),
    #[error("Unrecognized value type discriminator")]
    UnrecognizedValueTypeDiscriminator(u8),
    #[error("Failed to read key")]
    FailedToReadKey
}