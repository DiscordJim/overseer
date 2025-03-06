use std::{borrow::Cow, io::ErrorKind};



use crate::{
    access::{WatcherActivity, WatcherBehaviour},
    error::NetworkError,
    models::{Key, LocalReadAsync, LocalWriteAsync, Value},
};

use super::{OvrInteger, Packet, PacketId, PacketPayload, CURRENT_VERSION};




// Encoder

pub(crate) async fn write_packet<'a, W>(packet: &Packet<'a>, socket: &mut W) -> Result<(), NetworkError>
where
    W: LocalWriteAsync,
{
    socket.write_u8(CURRENT_VERSION).await?;
    socket.write_u32(packet.id().id()).await?;
    socket.write_u32(packet.id().order()).await?;
    // socket.write_i64(packet.id()).await?;
    socket.write_u8(packet.payload().discriminator()).await?;
    match packet.payload() {
        PacketPayload::Get { key } => write_get_packet(key, socket).await,
        PacketPayload::Insert { key, value } => write_insert_packet(key, value, socket).await,
        PacketPayload::Release { key } => write_release_packet(key, socket).await,
        PacketPayload::Watch {
            key,
            activity,
            behaviour,
        } => write_watch_packet(key, activity, behaviour, socket).await,
        PacketPayload::Delete { key } => write_delete_packet(key, socket).await,
        PacketPayload::Notify { key, value, more } => write_notify_packet(key, value.as_deref(), *more, socket).await,
        PacketPayload::Return { key, value } => write_getreturn_packet(key, value.as_deref(), socket).await,
    }
}

/// Reads a packet by deferring to submethods.
pub(crate) async fn read_packet<R>(socket: &mut R) -> Result<Packet<'static>, NetworkError>
where
    R: LocalReadAsync
{

    let version = socket.read_u8().await?;

    let id_first = socket.read_u32().await?;
    let id_second = socket.read_u32().await?;

    // println!("{version} {id_first} {id_second}");
    // let id = socket.read_i64().await?;

    Ok(Packet::new(
        PacketId::new(id_first, id_second),
        match version {
            0 => read_packet_v0(socket).await?,
            x => Err(NetworkError::UnknownPacketSchema(x))?,
        }
    ))
}

async fn read_packet_v0<'a, R>(socket: &mut R) -> Result<PacketPayload<'a>, NetworkError>
where
    R: LocalReadAsync,
{
    let discrim = socket.read_u8().await?;
    match discrim {
        0 => read_set_packet(socket).await,
        1 => read_get_packet(socket).await,
        2 => read_watch_packet(socket).await,
        3 => read_release_packet(socket).await,
        4 => read_delete_packet(socket).await,
        5 => read_notify_packet(socket).await,
        6 => read_getreturn_packet(socket).await,
        x => Err(NetworkError::UnrecognizedPacketTypeDiscriminator(x)),
    }
}

async fn write_getreturn_packet<'a, W>(
    key: &Key,
    val: Option<&'a Value>,
    socket: &mut W,
) -> Result<(), NetworkError>
where
    W: LocalWriteAsync,
{
    key.serialize(socket).await?;
    val.serialize(socket).await?;
    Ok(())
}

async fn write_notify_packet<'a, W>(
    key: &'a Key,
    value: Option<&'a Value>,
    more: bool,
    socket: &mut W,
) -> Result<(), NetworkError>
where
    W: LocalWriteAsync,
{
    key.serialize(socket).await?;
    value.serialize(socket).await?;
    more.serialize(socket).await?;
    Ok(())
}



impl OverseerSerde<bool> for bool {
    type E = std::io::Error;
    async fn deserialize<R: LocalReadAsync>(reader: &mut R) -> Result<bool, Self::E> {
        Ok(match reader.read_u8().await? {
            0 => false,
            1 => true,
            _ => Err(std::io::Error::new(ErrorKind::InvalidData, "Could not decode boolean."))?,
        })
    }
    async fn serialize<W: LocalWriteAsync>(&self, writer: &mut W) -> Result<(), Self::E> {
        if *self {
            writer.write_all(vec![  1 ]).await?;
        } else {
            writer.write_all(vec![ 0 ]).await?;
        }
        Ok(())
    }
}



// async fn write_optional_value<'a, W>(val: Option<&'a Value>, writer: &mut W) -> Result<(), NetworkError>
// where
//     W: LocalWriteAsync,
// {
//     match val {
//         Some(v) => {
//             writer.write_all(vec![ 1 ]).await?;
//             v.serialize(writer).await?;
//         }
//         None => {
//             writer.write_all(vec![ 0 ]).await?;
//         }
//     }
//     Ok(())
// }

// async fn read_optional_value<R>(reader: &mut R) -> Result<Option<Value>, NetworkError>
// where
//     R: LocalReadAsync,
// {
//     Ok(match reader.read_u8().await? {
//         0 => None,
//         1 => Some(Value::deserialize(reader).await?),
//         _ => Err(NetworkError::ErrorDecodingOption)?,
//     })
// }

async fn write_watch_packet<W: LocalWriteAsync>(
    key: &Key,
    activity: &WatcherActivity,
    behaviour: &WatcherBehaviour,
    socket: &mut W,
) -> Result<(), NetworkError> {
    key.serialize(socket).await?;
    socket
        .write_all([activity.discriminator(), behaviour.discriminator()].to_vec())
        .await?;
    Ok(())
}

async fn write_insert_packet<'a, W: LocalWriteAsync>(
    key: &'a Cow<'a, Key>,
    value: &'a Cow<'a, Value>,
    socket: &mut W,
) -> Result<(), NetworkError> {
    key.serialize(socket).await?;
    value.serialize(socket).await?;
    Ok(())
}

// pub(crate) async fn write_value<'a, W: LocalWriteAsync>(
//     value: &'a Value,
//     socket: &mut W,
// ) -> Result<(), NetworkError> {
//     match &*value {
//         Value::String(s) => write_value_string(&*s, socket).await,
//         Value::Integer(s) => write_value_signed_integer(*s, socket).await,
//     }
// }

#[inline]
async fn write_value_string<'a, W: LocalWriteAsync>(
    value: &'a str,
    socket: &mut W,
) -> Result<(), NetworkError> {
    socket.write_all(vec![ 0 ]).await?;
    value.serialize(socket).await?;
    Ok(())
}

// #[inline]
// async fn write_string<'a, W: LocalWriteAsync>(
//     value: &'a str,
//     socket: &mut W,
// ) -> Result<(), NetworkError> {
    
//     Ok(())
// }

async fn write_value_signed_integer<W: LocalWriteAsync>(
    value: i64,
    socket: &mut W,
) -> Result<(), NetworkError> {
    socket.write_all([1].to_vec()).await?;
    OvrInteger::write(value, socket).await?;
    // socket.write_all(value.to_be_bytes().to_vec()).await?;
    Ok(())
}

async fn write_delete_packet<W: LocalWriteAsync>(
    key: &Key,
    socket: &mut W,
) -> Result<(), NetworkError> {
    key.serialize(socket).await?;
    Ok(())
}

async fn write_get_packet<W: LocalWriteAsync>(
    key: &Key,
    socket: &mut W,
) -> Result<(), NetworkError> {
    key.serialize(socket).await?;
    Ok(())
}

async fn write_release_packet<W: LocalWriteAsync>(
    key: &Key,
    socket: &mut W,
) -> Result<(), NetworkError> {
    key.serialize(socket).await?;
    Ok(())
}

// #[inline]
// pub(crate) async fn write_key<'a, W: LocalWriteAsync>(
//     key: &'a Key,
//     socket: &mut W,
// ) -> Result<(), NetworkError> {
//    write_string(key.as_str(), socket).await
// }

// Decoder

/// Reads a packet of the set type.
async fn read_getreturn_packet<'a, R: LocalReadAsync>(
    socket: &mut R,
) -> Result<PacketPayload<'a>, NetworkError> {
    let key = Key::deserialize(socket).await?;
    let value = Option::<&Value>::deserialize(socket).await?;
    Ok(PacketPayload::Return { key: Cow::Owned(key), value: value.map(|f| Cow::Owned(f)) })
}
/// Reads a packet of the set type.
async fn read_notify_packet<'a, R: LocalReadAsync>(socket: &mut R) -> Result<PacketPayload<'a>, NetworkError> {
    let key = Key::deserialize(socket).await?;
    let value = Option::<&Value>::deserialize(socket).await?;
    let more = bool::deserialize(socket).await?;
    Ok(PacketPayload::Notify { key: Cow::Owned(key), value: value.map(|f| Cow::Owned(f)), more })
}

/// Reads a packet of the set type.
async fn read_delete_packet<'a, R: LocalReadAsync>(socket: &mut R) -> Result<PacketPayload<'a>, NetworkError> {
    println!("Reading delete packet...");
    let key = Key::deserialize(socket).await?;
    Ok(PacketPayload::Delete { key: Cow::Owned(key) })
}

/// Reads a packet of the set type.
async fn read_release_packet<'a, R: LocalReadAsync>(socket: &mut R) -> Result<PacketPayload<'a>, NetworkError> {
    let key = Key::deserialize(socket).await?;
    Ok(PacketPayload::Release { key: Cow::Owned(key) })
}

/// Reads a packet of the set type.
async fn read_get_packet<'a, R: LocalReadAsync>(socket: &mut R) -> Result<PacketPayload<'a>, NetworkError> {
    let key = Key::deserialize(socket).await?;
    Ok(PacketPayload::Get { key: Cow::Owned(key) })
}

/// Reads a packet of the set type.
async fn read_set_packet<'a, R: LocalReadAsync>(socket: &mut R) -> Result<PacketPayload<'a>, NetworkError> {
    let key = Key::deserialize(socket).await?;
    let value = Value::deserialize(socket).await?;
    Ok(PacketPayload::Insert { key: Cow::Owned(key), value: Cow::Owned(value) })
}

/// Reads a packet of the set type.
async fn read_watch_packet<'a, R: LocalReadAsync>(socket: &mut R) -> Result<PacketPayload<'a>, NetworkError> {
    let key = Key::deserialize(socket).await?;
    let activity = WatcherActivity::try_from(socket.read_u8().await?)?;
    let behaviour = WatcherBehaviour::try_from(socket.read_u8().await?)?;
    Ok(PacketPayload::Watch {
        key: Cow::Owned(key),
        activity,
        behaviour,
    })
}


// pub(crate) async fn read_value<R: LocalReadAsync>(socket: &mut R) -> Result<Value, NetworkError> {
//     let type_discrim = socket.read_u8().await?;
//     match type_discrim {
//         0 => Ok(Value::String(<&str>::deserialize(socket).await?)),
//         1 => decode_value_signed_integer(socket).await,
//         x => Err(NetworkError::UnrecognizedValueTypeDiscriminator(x)),
//     }
// }

async fn decode_value_signed_integer<R: LocalReadAsync>(socket: &mut R) -> Result<Value, NetworkError> {
    let val: i64 = OvrInteger::read(socket).await?;
    Ok(Value::Integer(val))
}



// pub(crate) async fn read_key<R>(socket: &mut R) -> Result<Key, NetworkError>
// where 
//     R: LocalReadAsync
// {
    
// }

#[allow(async_fn_in_trait)]
pub trait OverseerSerde<O: Sized>: Sized {
    type E;
    async fn serialize<W: LocalWriteAsync>(&self, writer: &mut W) -> Result<(), Self::E>;
    async fn deserialize<R: LocalReadAsync>(reader: &mut R) -> Result<O, Self::E>;
}

impl OverseerSerde<Value> for Value {
    type E = NetworkError;
    async fn deserialize<R: LocalReadAsync>(reader: &mut R) -> Result<Value, Self::E> {
        let type_discrim = reader.read_u8().await?;
        match type_discrim {
            0 => Ok(Value::String(<&str>::deserialize(reader).await?)),
            1 => decode_value_signed_integer(reader).await,
            x => Err(NetworkError::UnrecognizedValueTypeDiscriminator(x)),
        }
    }
    async fn serialize<W: LocalWriteAsync>(&self, writer: &mut W) -> Result<(), Self::E> {
        match &*self {
            Value::String(s) => write_value_string(&*s, writer).await,
            Value::Integer(s) => write_value_signed_integer(*s, writer).await,
        }
    }
}

impl<'a, J, O> OverseerSerde<Option<O>> for Option<&'a J>
where 
    J: OverseerSerde<O>,
    O: Sized,
    <J as OverseerSerde<O>>::E: From<std::io::Error>
{
    type E = J::E;
    async fn deserialize<R: LocalReadAsync>(reader: &mut R) -> Result<Option<O>, Self::E> {
        let flag = reader.read_u8().await?;
        if flag == 0 {
            Ok(None)
        } else if flag == 1 {
            Ok(Some(J::deserialize(reader).await?))
        } else {
            Err(std::io::Error::new(ErrorKind::InvalidData, "Failed decoding option"))?
        }
    }
    async fn serialize<W: LocalWriteAsync>(&self, writer: &mut W) -> Result<(), Self::E> {
        match self {
            None => writer.write_u8(0).await?,
            Some(i) => {
                writer.write_u8(1).await?;
                i.serialize(writer).await?;
            }
        }
        Ok(())
    }
    
}

impl<'a> OverseerSerde<String> for &'a str {
    type E = NetworkError;
    async fn serialize<W: LocalWriteAsync>(&self, writer: &mut W) -> Result<(), Self::E> {
        OvrInteger::write(self.len(), writer).await?;
        writer.write_all(self.as_bytes().to_vec()).await?;
        Ok(())
    }
    async fn deserialize<R: LocalReadAsync>(reader: &mut R) -> Result<String, Self::E> {
        // Figure out the size of the string.
        let string_length: u64 = OvrInteger::read(reader).await?;

        if string_length == 0 {
            return Ok(String::default());
        }

        let mut str_buf = vec![0u8; string_length as usize];
        reader.read_exact(&mut str_buf).await?;

        Ok(
            String::from_utf8(str_buf).map_err(|_| NetworkError::FailedToReadValue)?,
        )
    }
}

impl OverseerSerde<Key> for Key {
    type E = NetworkError;
    async fn serialize<W: LocalWriteAsync>(&self, writer: &mut W) -> Result<(), Self::E> {
        self.as_str().serialize(writer).await?;
        Ok(())
    }
    async fn deserialize<R: LocalReadAsync>(reader: &mut R) -> Result<Self, Self::E> {
        Ok(Key::from_owned(<&str>::deserialize(reader).await?))
    }
}

// #[async_trait::async_trait]
// impl OverseerSerde for Key {
//     type E = NetworkError;
//     async fn serialize<W: LocalWriteAsync>(&self, writer: &mut W) -> Result<(), E> {
//         if let Value::String(inner) = decode_value_string(socket).await? {
//             Ok(Key::from_str(&inner ))
//         } else {
//             Err(NetworkError::FailedToReadKey)
//         }
//     }
//     async fn deserialize<W: LocalReadAsync>(writer: &mut W) -> std::io::Result<Key> {
//         if let Value::String(inner) = decode_value_string(socket).await? {
//             Ok(Key::from_str(&inner ))
//         } else {
//             Err(NetworkError::FailedToReadKey)
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};

  

    use crate::{
        access::{WatcherActivity, WatcherBehaviour},
        models::{Key, LocalWriteAsync, Value},
        network::{decoder::{
            read_packet,
            write_packet,
        }, OverseerSerde, OvrInteger, PacketId, PacketPayload},
    };

    use super::Packet;

    // use crate::net::{driver::read_packet, Driver};

    #[tokio::test]
    pub async fn read_bool_test() {
        let mut cursor = Cursor::new(vec![0, 1]);
        assert_eq!(bool::deserialize(&mut cursor).await.unwrap(), false);
        assert_eq!(bool::deserialize(&mut cursor).await.unwrap(), true);
    }

    #[tokio::test]
    pub async fn read_optional_value_test() {
        // Write a null.
        let mut cursor = Cursor::new(vec![]);
        LocalWriteAsync::write_all(&mut cursor, vec![0u8, 1u8, 1u8]).await.unwrap();
        OvrInteger::write(64i64, &mut cursor).await.unwrap();
        cursor.set_position(0);



        assert_eq!(Option::<&Value>::deserialize(&mut cursor).await.unwrap(), None);
        assert_eq!(
            Option::<&Value>::deserialize(&mut cursor).await.unwrap(),
            Some(Value::Integer(64))
        );
    }

    #[tokio::test]
    pub async fn write_optional_value_test() {
        // Write a null.
        let mut cursor = vec![];
        None::<&Value>.serialize(&mut cursor).await.unwrap();
        // write_optional_value(None, &mut cursor).await.unwrap();
        assert_eq!(cursor.len(), 1);
        assert_eq!(cursor[0], 0);

        // Write some value
        let mut cursor = Cursor::new(vec![]);
        Some(&Value::Integer(22)).serialize(&mut cursor).await.unwrap();
        // assert_eq!(cursor.len(), 3);
        cursor.set_position(2);
        assert_eq!(OvrInteger::read::<i64, _>(&mut cursor).await.unwrap(), 22);
    }

    #[tokio::test]
    pub async fn write_bool_test() {
        let mut cursor = vec![];
        true.serialize(&mut cursor).await.unwrap();
        assert_eq!(cursor[0], 1);
        false.serialize(&mut cursor).await.unwrap();
        assert_eq!(cursor[1], 0);
    }

    #[tokio::test]
    pub async fn write_notify_packet() {
        // let packet = Packet::new(PacketId::zero(), PacketPayload::Notify {
        //     key: Key::from_str("hello"),
        //     value: None,
        //     more: false,
        // });
        let key = Key::from_str("hello");
        let packet = Packet::new(PacketId::zero(), PacketPayload::notify(&key, None, false));

        // Write the packet.
        let mut cursor = Cursor::new(vec![]);
        packet.write(&mut cursor).await.unwrap();
        cursor.set_position(0);

        if let PacketPayload::Notify { key, value, more } = read_packet(&mut cursor).await.unwrap().payload() {
            assert_eq!(key.as_str(), "hello");
            assert!(value.is_none());
            assert!(!more);
        } else {
            panic!("Wrong packet type.");
        }
    }

    #[tokio::test]
    pub async fn write_delete_packet() {
        let key = Key::from_str("hello");
        let packet = Packet::new(PacketId::zero(), PacketPayload::delete(&key));

        // Write the packet.
        let mut cursor = Cursor::new(vec![]);
        packet.write(&mut cursor).await.unwrap();
        cursor.set_position(0);

        if let PacketPayload::Delete { key } = read_packet(&mut cursor).await.unwrap().payload() {
            assert_eq!(key.as_str(), "hello");
        } else {
            panic!("Wrong packet type.");
        }
    }

    #[tokio::test]
    pub async fn write_release_packet() {
        let key = Key::from_str("hello");
        let packet = Packet::new(PacketId::zero(), PacketPayload::release(&key));

        // Write the packet.
        let mut cursor = Cursor::new(vec![]);
        packet.write(&mut cursor).await.unwrap();
        cursor.set_position(0);

        if let PacketPayload::Release { key } = read_packet(&mut cursor).await.unwrap().payload() {
            assert_eq!(key.as_str(), "hello");
        } else {
            panic!("Wrong packet type.");
        }
    }

    #[tokio::test]
    pub async fn write_watch_packet() {
        let key = Key::from_str("hello");
        let packet = Packet::new(PacketId::zero(), PacketPayload::watch(
            &key,
            WatcherActivity::Lazy,
            WatcherBehaviour::Eager
        ));

        // Write the packet.
        let mut cursor = Cursor::new(vec![]);
        write_packet(&packet, &mut cursor).await.unwrap();
        cursor.set_position(0);

        if let PacketPayload::Watch {
            key,
            activity,
            behaviour,
        } = read_packet(&mut cursor).await.unwrap().payload()
        {
            assert_eq!(key.as_str(), "hello");
            assert_eq!(*activity, WatcherActivity::Lazy);
            assert_eq!(*behaviour, WatcherBehaviour::Eager);
        } else {
            panic!("Wrong packet type.");
        }
    }

    #[tokio::test]
    pub async fn write_insert_string_packet() {

        let key = Key::from_str("hello");
        let value = Value::String("hello world".to_string());
        let packet = Packet::new(PacketId::zero(), PacketPayload::insert(&key, &value));

        // Write the packet.
        let mut cursor = Cursor::new(vec![]);
        write_packet(&packet, &mut cursor).await.unwrap();
        cursor.set_position(0);

        if let PacketPayload::Insert { key, value } = read_packet(&mut cursor).await.unwrap().payload() {
            assert_eq!(key.as_str(), "hello");
            assert_eq!(value.as_string().unwrap(), "hello world");
        } else {
            panic!("Wrong packet type.");
        }
    }

    #[tokio::test]
    pub async fn write_insert_integer_packet() {
        let key = Key::from_str("hello");
        let value = Value::Integer(32);
        let packet = Packet::new(PacketId::zero(), PacketPayload::insert(&key, &value));

        // Write the packet.
        let mut cursor = Cursor::new(vec![]);
        write_packet(&packet, &mut cursor).await.unwrap();
        cursor.set_position(0);

        if let PacketPayload::Insert { key, value } = read_packet(&mut cursor).await.unwrap().payload() {
            assert_eq!(key.as_str(), "hello");
            assert_eq!(value.as_integer().unwrap(), 32);
        } else {
            panic!("Wrong packet type.");
        }
    }

    #[tokio::test]
    pub async fn write_get_packet() {
        let key = Key::from_str("hello");
        let packet = Packet::new(PacketId::zero(), PacketPayload::get(&key));

        // Write the packet.
        let mut cursor = Cursor::new(vec![]);
        write_packet(&packet, &mut cursor).await.unwrap();
        cursor.set_position(0);

        if let PacketPayload::Get { key } = read_packet(&mut cursor).await.unwrap().payload() {
            assert_eq!(key.as_str(), "hello");
        } else {
            panic!("Wrong packet type.");
        }
    }

    #[tokio::test]
    pub async fn read_release_packet() {
        let skey = "hello";
        let mut buffer = vec![0u8, 0, 0, 0, 0, 0, 0, 0, 0, 3u8];
        OvrInteger::write(skey.as_bytes().len(), &mut buffer).await.unwrap();
        buffer.extend_from_slice(skey.as_bytes());

        if let PacketPayload::Release { key } = read_packet(&mut Cursor::new(buffer)).await.unwrap().payload() {
            assert_eq!(**key, Key::from_str(skey));
        } else {
            panic!("Packet did not decode as the proper type.");
        }
    }

    #[tokio::test]
    pub async fn read_notify_packet() {
        let skey = "hello";
        let mut buffer = vec![0u8, 0, 0, 0, 0, 0, 0, 0, 0, 5u8];
        OvrInteger::write(skey.as_bytes().len(), &mut buffer).await.unwrap();
        buffer.extend_from_slice(skey.as_bytes());

        // 1 = Some
        // 1 = Integer
        // 64 0 0 0 0 0 0 0 = A i64 of 64
        // 1 = True
        LocalWriteAsync::write_all(&mut buffer, vec![1, 1]).await.unwrap();
        OvrInteger::write(64, &mut buffer).await.unwrap();
        LocalWriteAsync::write_all(&mut buffer, vec![1]).await.unwrap();
        // buffer.extend_from_slice(&vec![1, 1].into_iter().chain(Ov).chain(vec![1]).collect::<Vec<u8>>());

        if let PacketPayload::Notify { key, value, more } =
            Packet::read(&mut Cursor::new(buffer)).await.unwrap().payload()
        {
            assert_eq!(**key, Key::from_str(skey));
            assert_eq!(**value.as_ref().unwrap(), Value::Integer(64));
            assert!(more);
        } else {
            panic!("Packet did not decode as the proper type.");
        }
    }

    #[tokio::test]
    pub async fn read_watch_packet() {
        let skey = "hello";
        let mut buffer = vec![0u8, 0, 0, 0, 0, 0, 0, 0, 0, 2u8];
        OvrInteger::write(skey.as_bytes().len(), &mut buffer).await.unwrap();
        buffer.extend_from_slice(skey.as_bytes());

        buffer.push(1);
        buffer.push(0);

        if let PacketPayload::Watch {
            key,
            activity,
            behaviour,
        } = read_packet(&mut Cursor::new(buffer)).await.unwrap().payload()
        {
            assert_eq!(**key, Key::from_str(skey));
            assert_eq!(*activity, WatcherActivity::Lazy);
            assert_eq!(*behaviour, WatcherBehaviour::Ordered);
        } else {
            panic!("Packet did not decode as the proper type.");
        }
    }

    #[tokio::test]
    pub async fn read_delete_packet() {
        let skey = "hello";
        let mut buffer = vec![0u8, 0, 0, 0, 0, 0, 0, 0, 0, 4u8];
        OvrInteger::write(skey.as_bytes().len(), &mut buffer).await.unwrap();
        buffer.extend_from_slice(skey.as_bytes());

        println!("Hello");

        if let PacketPayload::Delete { key } = Packet::read(&mut Cursor::new(buffer)).await.unwrap().payload() {
            assert_eq!(**key, Key::from_str(skey));
        } else {
            panic!("Packet did not decode as the proper type.");
        }
    }

    #[tokio::test]
    pub async fn read_get_packet() {
        let skey = "hello";
        let mut buffer = vec![0u8, 0, 0, 0, 0, 0, 0, 0, 0, 1u8];
        OvrInteger::write(skey.as_bytes().len(), &mut buffer).await.unwrap();
        // buffer.extend_from_slice(&(skey.as_bytes().len() as u32).to_be_bytes());
        buffer.extend_from_slice(skey.as_bytes());

        if let PacketPayload::Get { key } = read_packet(&mut Cursor::new(buffer)).await.unwrap().payload() {
            assert_eq!(**key, Key::from_str(skey));
        } else {
            panic!("Packet did not decode as the proper type.");
        }
    }

    #[tokio::test]
    pub async fn read_integer_insert_packet() {
        let skey = "hello";
        let mut buffer = vec![0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0u8];
        OvrInteger::write(skey.as_bytes().len(), &mut buffer).await.unwrap();
        buffer.extend_from_slice(skey.as_bytes());

        // let svalue: i64 = 382;
        buffer.push(1);
        OvrInteger::write(382i64, &mut buffer).await.unwrap();
        // buffer.extend_from_slice(&svalue.to_be_bytes());

        if let PacketPayload::Insert { key, value } = read_packet(&mut Cursor::new(buffer)).await.unwrap().payload()
        {
            assert_eq!(**key, Key::from_str(skey));
            assert_eq!(value.as_integer().unwrap(), 382);
        } else {
            panic!("Packet did not decode as the proper type.");
        }
    }

    #[tokio::test]
    pub async fn read_string_insert_packet() {
        let skey = "hello";
        let mut buffer = vec![0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0u8];
        OvrInteger::write(skey.as_bytes().len(), &mut buffer).await.unwrap();
        buffer.extend_from_slice(skey.as_bytes());

        let svalue = "I am a string to be set.";
        buffer.push(0);
        OvrInteger::write(svalue.as_bytes().len(), &mut buffer).await.unwrap();
        buffer.extend_from_slice(svalue.as_bytes());

        if let PacketPayload::Insert { key, value } = read_packet(&mut Cursor::new(buffer)).await.unwrap().payload()
        {
            assert_eq!(**key, Key::from_str(skey));
            assert_eq!(value.as_string().unwrap(), svalue);
        } else {
            panic!("Packet did not decode as the proper type.");
        }
    }
}
