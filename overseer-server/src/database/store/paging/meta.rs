use overseer::error::NetworkError;

use crate::database::store::file::{PAGE_SIZE, RESERVED_HEADER_SIZE};

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct RawPageAddress(u32);

impl RawPageAddress {
    pub fn new(inner: u32) -> Self {
        Self(inner)
    }
    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }
    pub fn as_u64(&self) -> u64 {
        self.0 as u64
    }
    pub fn zero() -> Self {
        Self::new(0)
    }
    pub fn offset(self, o: u32) -> Self
    {
        Self(self.0 + o)
    }
    pub fn offset_subtract(self, o: u32) -> Self {
        Self(self.0 - o)
    }
    pub fn page_number(&self) -> u32 {
        if self.is_zero() {
            0
        } else {
            (self.0 - RESERVED_HEADER_SIZE) / PAGE_SIZE as u32
        }
        
    }
}



#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum PageType {
    Normal,
    Dummy
}

impl PageType {
    pub fn as_u8(&self) -> u8 {
        match self {
            Self::Normal => 0,
            Self::Dummy => 1
        }
    }
    pub fn from_u8(discrim: u8) -> Result<Self, NetworkError> {
        Ok(match discrim {
            0 => Self::Normal,
            1 => Self::Dummy,
            _ => Err(NetworkError::ErrorDecodingBoolean)?
        })
    }
}