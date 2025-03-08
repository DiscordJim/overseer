

use std::array::TryFromSliceError;

use overseer::error::NetworkError;
use thiserror::Error;



#[derive(Error, Debug)]
pub enum PageError {
    #[error("Wrong response from server")]
    LeafPageFull,
    #[error("IO Error")]
    IoError(#[from] std::io::Error),
    #[error("No record found")]
    NoRecordFound,
    #[error("Failed to read a free block into memory")]
    FailedReadingFreeBlock,
    #[error("Allocation details did not make sense")]
    BadAllocation
}

// impl Into<NetworkError> for PageError {
//     fn into(self) -> NetworkError {
//         NetworkError::PagingError(format!("{self:?}"))
//     }
// }

impl From<PageError> for NetworkError {
    fn from(value: PageError) -> Self {
        value.into()
    }
}