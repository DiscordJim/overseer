

use std::array::TryFromSliceError;

use thiserror::Error;



#[derive(Error, Debug)]
pub enum PageError {
    #[error("Wrong response from server")]
    LeafPageFull,
    #[error("IO Error")]
    IoError(#[from] std::io::Error)
}