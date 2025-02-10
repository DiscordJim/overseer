use std::borrow::Borrow;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{access::{WatcherActivity, WatcherBehaviour}, error::NetworkError, models::{Key, Value}};

use super::decoder::{read_packet, write_packet};

pub const CURRENT_VERSION: u8 = 0;

pub struct Packet {
    id: PacketId,
    payload: PacketPayload 
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PacketId(u32, u32);

impl PacketId {
    pub fn zero() -> Self {
        Self(0, 0)
    }
    pub fn new(id: u32, order: u32) -> Self {
        Self(id, order)
    }
    pub fn id(&self) -> u32 {
        self.0
    }
    pub fn order(&self) -> u32 {
        self.1
    }
}

impl Packet {
    pub fn new(id: PacketId, payload: PacketPayload) -> Self {
        Self {
            id,
            payload
        }
    }
    pub fn id(&self) -> PacketId {
        self.id
    }
    pub fn payload(&self) -> &PacketPayload {
        &self.payload
    }
    
    pub async fn write<W>(&self, writer: &mut W) -> Result<(), NetworkError>
    where 
        W: AsyncWrite + Unpin
    {
        write_packet(self, writer).await
    }
    pub async fn read<R>(reader: &mut R) -> Result<Packet, NetworkError>
    where
        R: AsyncRead + Unpin
    {
        read_packet(reader).await
    }
    pub fn get<K>(id: PacketId, key: K) -> Self
    where 
        K: Borrow<Key>
    {
        Self {
            id,
            payload: PacketPayload::Get { key: key.borrow().to_owned() }
        }
    }
    pub fn delete<K>(id: PacketId, key: K) -> Self
    where 
        K: Borrow<Key>
    {
        Self {
            id,
            payload: PacketPayload::Delete { key: key.borrow().to_owned() }
        }
    }
    pub fn insert<K>(id: PacketId, key: K, value: Value) -> Self
    where 
        K: Borrow<Key>
    {
        Self {
            id,
            payload: PacketPayload::Insert { key: key.borrow().to_owned(), value }
        }
    }
    pub fn release<K>(id: PacketId, key: K) -> Self
    where 
        K: Borrow<Key>
    {
        Self {
            id,
            payload: PacketPayload::Release { key: key.borrow().to_owned() }
        }
    }
    pub fn watch<K>(
        id: PacketId,
        key: K,
        activity: WatcherActivity,
        behaviour: WatcherBehaviour
    ) -> Self
    where 
        K: Borrow<Key>
    {
        Self {
            id,
            payload: PacketPayload::Watch { key: key.borrow().to_owned(), activity, behaviour }
        }
    }
    pub fn vreturn<K>(
        id: PacketId,
        key: K,
        value: Option<Value>
    ) -> Self
    where 
        K: Borrow<Key>
    {
        Self {
            id,
            payload: PacketPayload::Return { key: key.borrow().to_owned(), value }
        }
    }
    pub fn notify<K>(
        id: PacketId,
        key: K,
        value: Option<Value>,
        is_more: bool
    ) -> Self
    where 
        K: Borrow<Key>
    {
        Self {
            id,
            payload: PacketPayload::Notify { key: key.borrow().to_owned(), value, more: is_more }
        }
    }
}

//pub async fn write<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<(), NetworkError> {
//     write_packet(self, writer).await
// }
// pub async fn read<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self, NetworkError> {
//     read_packet(reader).await
// }

#[derive(Debug)]
pub enum PacketPayload {
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


impl PacketPayload {
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
    
}
