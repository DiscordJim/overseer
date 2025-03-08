use thiserror::Error;

#[derive(Error, Debug)]
pub enum FrameAllocatorError {
    #[error("The size of the frames is not a power of two")]
    BadFrameSize,
    #[error("Requested a frame that is out of bounds.")]
    FrameOutOfBounds,
    #[error("Requested a frame that was already in use")]
    FrameInUse
}