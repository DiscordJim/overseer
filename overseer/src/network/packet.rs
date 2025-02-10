use std::borrow::Borrow;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{access::{WatcherActivity, WatcherBehaviour}, error::NetworkError, models::{Key, Value}};

use super::decoder::{read_packet, write_packet};

pub const CURRENT_VERSION: u8 = 0;

#[derive(Debug)]
pub enum Packet {
    Insert {
        key: Key,
        value: Value
    },
    Get {
        key: Key
    },
    Watch {
        key: Key,
        activity: WatcherActivity,
        behaviour: WatcherBehaviour
    },
    Release {
        key: Key
    },
    Delete {
        key: Key
    },
    Notify {
        key: Key,
        value: Option<Value>,
        more: bool
    },
    Return {
        key: Key,
        value: Option<Value>
    }
}


impl Packet {
    pub fn notify<K: Into<Key>, V: Into<Value>>(key: K, value: Option<V>, more: bool) -> Self {
        Self::Notify { key: key.into(), value: value.map(Into::into), more }

    }
    pub fn release<K: Into<Key>>(key: K) -> Self {
        Self::Release { key: key.into() }
    }
    pub fn watch<K: Borrow<Key>>(key: K, activity: WatcherActivity, behaviour: WatcherBehaviour) -> Self {
        Self::Watch { key: key.borrow().to_owned(), activity, behaviour }
    }
    pub fn insert<K: Borrow<Key>, V: Into<Value>>(key: K, value: V) -> Self {
        Self::Insert { key: key.borrow().to_owned(), value: value.into() }
    }
    pub fn delete<K: Borrow<Key>>(key: K) -> Self {
        Self::Delete { key: key.borrow().to_owned() }
    }
    pub fn get<K: Borrow<Key>>(key: K) -> Self {
        Self::Get { key: key.borrow().to_owned() }
    }
    pub fn discriminator(&self) -> u8 {
        match self {
            Self::Insert { .. } => 0,
            Self::Get { .. } => 1,
            Self::Watch { .. } => 2,
            Self::Release { .. } => 3,
            Self::Delete { .. } => 4,
            Self::Notify { .. } => 5,
            Self::Return { .. } => 6
        }
    }
    pub async fn write<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<(), NetworkError> {
        write_packet(self, writer).await
    }
    pub async fn read<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self, NetworkError> {
        read_packet(reader).await
    }
}
