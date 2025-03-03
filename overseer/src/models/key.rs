use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{error::NetworkError, network::decoder::{read_key, write_key}};

use super::{LocalReadAsync, LocalWriteAsync};



#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Key(String);

impl Key {
    pub fn from_str<S: AsRef<str>>(key: S) -> Self {
        Self(key.as_ref().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub async fn read<R>(reader: &mut R) -> Result<Self, NetworkError>
    where 
        R: LocalReadAsync
    {
        read_key(reader).await
    }
    pub async fn write<W>(&self, writer: &mut W) -> Result<(), NetworkError>
    where 
        W: LocalWriteAsync
    {
        write_key(self, writer).await
    }
}


impl Into<Key> for &str {
    fn into(self) -> Key {
        Key(self.to_string())
    }
}

impl Into<Key> for String {
    fn into(self) -> Key {
        Key(self)
    }
}