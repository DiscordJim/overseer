use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{error::NetworkError};

use super::{LocalReadAsync, LocalWriteAsync};



#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Key(String);

impl Key {
    pub fn from_owned(key: String) -> Self {
        Self(key)
    }
    pub fn from_str<S: AsRef<str>>(key: S) -> Self {
        Self(key.as_ref().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
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