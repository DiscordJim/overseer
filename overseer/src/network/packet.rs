use std::borrow::{Borrow, Cow};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{access::{WatcherActivity, WatcherBehaviour}, error::NetworkError, models::{Key, LocalReadAsync, Value}};

use super::decoder::{read_packet, write_packet};

pub const CURRENT_VERSION: u8 = 0;

#[derive(Debug)]
pub struct Packet<'a> {
    id: PacketId,
    payload: PacketPayload<'a>
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
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



impl<'a> Packet<'a> {
    pub fn new(id: PacketId, payload: PacketPayload<'a>) -> Self {
        Self {
            id,
            payload
        }
    }
    pub fn id(&self) -> PacketId {
        self.id
    }
    pub fn payload(&self) -> &PacketPayload<'a> {
        &self.payload
    }
    
    pub fn into_payload(self) -> PacketPayload<'a> {
        self.payload
    }
    pub async fn write<W>(&self, writer: &mut W) -> Result<(), NetworkError>
    where 
        W: AsyncWrite + Unpin
    {
        write_packet(self, writer).await
    }
    pub async fn read<R>(reader: &mut R) -> Result<Packet<'static>, NetworkError>
    where
        R: LocalReadAsync
    {
        read_packet(reader).await
    }
    pub fn get(id: PacketId, key: &'a Key) -> Self
    {
        Self {
            id,
            payload: PacketPayload::get(key)
        }
    }
    pub fn delete(id: PacketId, key: &'a Key) -> Self
    {
        Self {
            id,
            payload: PacketPayload::delete(key)
        }
    }
    pub fn insert(id: PacketId, key: &'a Key, value: &'a Value) -> Self
    {
        Self {
            id,
            payload: PacketPayload::insert(key, value)
        }
    }
    pub fn release(id: PacketId, key: &'a Key) -> Self
    {
        Self {
            id,
            payload: PacketPayload::release(key)
        }
    }
    pub fn watch(
        id: PacketId,
        key: &'a Key,
        activity: WatcherActivity,
        behaviour: WatcherBehaviour
    ) -> Self
    {
        Self {
            id,
            payload: PacketPayload::watch(key, activity, behaviour)
        }
    }
    pub fn vreturn(
        id: PacketId,
        key: &'a Key,
        value: Option<&'a Value>
    ) -> Self
    {
        Self {
            id,
            payload: PacketPayload::return_packet(key, value)
        }
    }
    pub fn notify(
        id: PacketId,
        key: &'a Key,
        value: Option<&'a Value>,
        is_more: bool
    ) -> Self
    {
        Self {
            id,
            payload: PacketPayload::notify(key, value, is_more)
        }
    }
    pub fn to_owned(self) -> Packet<'static> {
        Packet {
            id: self.id,
            payload: self.payload.to_owned()
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
pub enum PacketPayload<'a> {
    Insert {
        key: Cow<'a, Key>,
        value: Cow<'a, Value>
    },
    Get {
        key: Cow<'a, Key>
    },
    Watch {
        key: Cow<'a, Key>,
        activity: WatcherActivity,
        behaviour: WatcherBehaviour
    },
    Release {
        key: Cow<'a, Key>
    },
    Delete {
        key: Cow<'a, Key>
    },
    Notify {
        key: Cow<'a, Key>,
        value: Option<Cow<'a, Value>>,
        more: bool
    },
    Return {
        key: Cow<'a, Key>,
        value: Option<Cow<'a, Value>>
    }
}


impl<'a> PacketPayload<'a> {
    pub fn notify(key: &'a Key, value: Option<&'a Value>, more: bool) -> Self {
        Self::Notify { key: Cow::Borrowed(key), value: value.map(|f| Cow::Borrowed(f)), more }

    }
    pub fn return_packet(key: &'a Key, value: Option<&'a Value>) -> Self {
        Self::Return { key: Cow::Borrowed(key), value: value.map(|f| Cow::Borrowed(f)) }
    }
    pub fn release(key: &'a Key) -> Self {
        Self::Release { key: Cow::Borrowed(key) }
    }
    pub fn watch(key: &'a Key, activity: WatcherActivity, behaviour: WatcherBehaviour) -> Self {
        Self::Watch { key: Cow::Borrowed(key), activity, behaviour }
    }
    pub fn insert(key: &'a Key, value: &'a Value) -> Self {
        Self::Insert { key: Cow::Borrowed(key), value: Cow::Borrowed(value) }
    }
    pub fn delete(key: &'a Key) -> Self {
        Self::Delete { key: Cow::Borrowed(key) }
    }
    pub fn get(key: &'a Key) -> Self {
        Self::Get { key: Cow::Borrowed(key) }
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
    pub fn to_owned(self) -> PacketPayload<'static> {
        // match self {
        //     Self::Delete { key } => Self::Delete { key: Cow::Owned(key.into_owned()) },
        //     Self::Get { key } => Self::Get { key: key.to_owned() },
        //     Self::Insert { key, value } => Self::Insert { key: key.to_owned(), value: value.to_owned() },
        //     Self::Notify { key, value, more } => Self::Notify { key: key.to_owned(), value: value.to_owned(), more },
        //     Self::Release { key } => Self::Release { key: key.to_owned() },
        //     Self::Watch { key, activity, behaviour } => Self::Watch { key: key.to_owned(), activity, behaviour },
        //     Self::Return { key, value } => Self::Return { key: key.to_owned(), value: value.map(|f| f.to_owned()) },

        // }
        own_packet_payload(self)
    }
    
}


// fn own_packet(packet: Packet<'_>) -> Packet<'static> {
//     Packet {
        
//     }
// }

fn own_packet_payload(payload: PacketPayload<'_>) -> PacketPayload<'static> {
    match payload {
        PacketPayload::Delete { key } => PacketPayload::Delete { key: Cow::Owned(key.into_owned()) },
        PacketPayload::Get { key } => PacketPayload::Get { key: Cow::Owned(key.into_owned()) },
        PacketPayload::Insert { key, value } => PacketPayload::Insert { key: Cow::Owned(key.into_owned()), value: Cow::Owned(value.into_owned()) },
        PacketPayload::Notify { key, value, more } => PacketPayload::Notify { key: Cow::Owned(key.into_owned()), value: own_value_cow(value), more },
        PacketPayload::Release { key } => PacketPayload::Release { key: Cow::Owned(key.into_owned()) },
        PacketPayload::Watch { key, activity, behaviour } => PacketPayload::Watch { key: Cow::Owned(key.into_owned()), activity, behaviour },
        PacketPayload::Return { key, value } => PacketPayload::Return { key: Cow::Owned(key.into_owned()), value: own_value_cow(value) },

    }
}

fn own_value_cow(value: Option<Cow<'_, Value>>) -> Option<Cow<'static, Value>> {
    match value {
        Some(v) => Some(Cow::Owned(v.into_owned())),
        None => None
    }
}