use std::borrow::Cow;



use crate::{
    access::{WatcherActivity, WatcherBehaviour},
    error::NetworkError,
    models::{Key, LocalReadAsync, LocalWriteAsync, Value},
};

use super::{Packet, PacketId, PacketPayload, CURRENT_VERSION};


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
    write_key(key, socket).await?;
    write_optional_value(val, socket).await?;
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
    write_key(&key, socket).await?;
    write_optional_value(value, socket).await?;
    write_bool(more, socket).await?;
    Ok(())
}

async fn write_bool<W>(val: bool, writer: &mut W) -> Result<(), NetworkError>
where
    W: LocalWriteAsync,
{
    if val {
        writer.write_all(vec![  1 ]).await?;
    } else {
        writer.write_all(vec![ 0 ]).await?;
    }
    Ok(())
}

async fn read_bool<R>(reader: &mut R) -> Result<bool, NetworkError>
where
    R: LocalReadAsync
{
    Ok(match reader.read_u8().await? {
        0 => false,
        1 => true,
        _ => Err(NetworkError::ErrorDecodingBoolean)?,
    })
}

async fn write_optional_value<'a, W>(val: Option<&'a Value>, writer: &mut W) -> Result<(), NetworkError>
where
    W: LocalWriteAsync,
{
    match val {
        Some(v) => {
            writer.write_all(vec![ 1 ]).await?;
            write_value(&v, writer).await?;
        }
        None => {
            writer.write_all(vec![ 0 ]).await?;
        }
    }
    Ok(())
}

async fn read_optional_value<R>(reader: &mut R) -> Result<Option<Value>, NetworkError>
where
    R: LocalReadAsync,
{
    Ok(match reader.read_u8().await? {
        0 => None,
        1 => Some(read_value(reader).await?),
        _ => Err(NetworkError::ErrorDecodingOption)?,
    })
}

async fn write_watch_packet<W: LocalWriteAsync>(
    key: &Key,
    activity: &WatcherActivity,
    behaviour: &WatcherBehaviour,
    socket: &mut W,
) -> Result<(), NetworkError> {
    write_key(&key, socket).await?;
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
    write_key(&key, socket).await?;
    write_value(&**value, socket).await?;
    Ok(())
}

pub(crate) async fn write_value<'a, W: LocalWriteAsync>(
    value: &'a Value,
    socket: &mut W,
) -> Result<(), NetworkError> {
    match &*value {
        Value::String(s) => write_value_string(&*s, socket).await,
        Value::Integer(s) => write_value_integer(*s, socket).await,
    }
}

async fn write_value_string<'a, W: LocalWriteAsync>(
    value: &'a str,
    socket: &mut W,
) -> Result<(), NetworkError> {
    socket.write_all(vec![ 0 ]).await?;
    socket
        .write_all((value.len() as u64).to_be_bytes().to_vec())
        .await?;
    socket.write_all(value.as_bytes().to_vec()).await?;
    Ok(())
}

async fn write_value_integer<W: LocalWriteAsync>(
    value: i64,
    socket: &mut W,
) -> Result<(), NetworkError> {
    socket.write_all([1].to_vec()).await?;
    socket.write_all(value.to_be_bytes().to_vec()).await?;
    Ok(())
}

async fn write_delete_packet<W: LocalWriteAsync>(
    key: &Key,
    socket: &mut W,
) -> Result<(), NetworkError> {
    write_key(&key, socket).await?;
    Ok(())
}

async fn write_get_packet<W: LocalWriteAsync>(
    key: &Key,
    socket: &mut W,
) -> Result<(), NetworkError> {
    write_key(&key, socket).await?;
    Ok(())
}

async fn write_release_packet<W: LocalWriteAsync>(
    key: &Key,
    socket: &mut W,
) -> Result<(), NetworkError> {
    write_key(&key, socket).await?;
    Ok(())
}

pub(crate) async fn write_key<'a, W: LocalWriteAsync>(
    key: &'a Key,
    socket: &mut W,
) -> Result<(), NetworkError> {
    socket
        .write_all((key.as_str().len() as u32).to_be_bytes().to_vec())
        .await?;
    socket.write_all(key.as_str().as_bytes().to_vec()).await?;
    Ok(())
}

// Decoder

/// Reads a packet of the set type.
async fn read_getreturn_packet<'a, R: LocalReadAsync>(
    socket: &mut R,
) -> Result<PacketPayload<'a>, NetworkError> {
    let key = read_key(socket).await?;
    let value = read_optional_value(socket).await?;
    Ok(PacketPayload::Return { key: Cow::Owned(key), value: value.map(|f| Cow::Owned(f)) })
}
/// Reads a packet of the set type.
async fn read_notify_packet<'a, R: LocalReadAsync>(socket: &mut R) -> Result<PacketPayload<'a>, NetworkError> {
    let key = read_key(socket).await?;
    let value = read_optional_value(socket).await?;
    let more = read_bool(socket).await?;
    Ok(PacketPayload::Notify { key: Cow::Owned(key), value: value.map(|f| Cow::Owned(f)), more })
}

/// Reads a packet of the set type.
async fn read_delete_packet<'a, R: LocalReadAsync>(socket: &mut R) -> Result<PacketPayload<'a>, NetworkError> {
    println!("Reading delete packet...");
    let key = read_key(socket).await?;
    Ok(PacketPayload::Delete { key: Cow::Owned(key) })
}

/// Reads a packet of the set type.
async fn read_release_packet<'a, R: LocalReadAsync>(socket: &mut R) -> Result<PacketPayload<'a>, NetworkError> {
    let key = read_key(socket).await?;
    Ok(PacketPayload::Release { key: Cow::Owned(key) })
}

/// Reads a packet of the set type.
async fn read_get_packet<'a, R: LocalReadAsync>(socket: &mut R) -> Result<PacketPayload<'a>, NetworkError> {
    let key = read_key(socket).await?;
    Ok(PacketPayload::Get { key: Cow::Owned(key) })
}

/// Reads a packet of the set type.
async fn read_set_packet<'a, R: LocalReadAsync>(socket: &mut R) -> Result<PacketPayload<'a>, NetworkError> {
    let key = read_key(socket).await?;
    let value = read_value(socket).await?;
    Ok(PacketPayload::Insert { key: Cow::Owned(key), value: Cow::Owned(value) })
}

/// Reads a packet of the set type.
async fn read_watch_packet<'a, R: LocalReadAsync>(socket: &mut R) -> Result<PacketPayload<'a>, NetworkError> {
    let key = read_key(socket).await?;
    let activity = WatcherActivity::try_from(socket.read_u8().await?)?;
    let behaviour = WatcherBehaviour::try_from(socket.read_u8().await?)?;
    Ok(PacketPayload::Watch {
        key: Cow::Owned(key),
        activity,
        behaviour,
    })
}

pub(crate) async fn read_value<R: LocalReadAsync>(socket: &mut R) -> Result<Value, NetworkError> {
    let type_discrim = socket.read_u8().await?;
    match type_discrim {
        0 => decode_value_string(socket).await,
        1 => decode_value_integer(socket).await,
        x => Err(NetworkError::UnrecognizedValueTypeDiscriminator(x)),
    }
}

async fn decode_value_integer<R: LocalReadAsync>(socket: &mut R) -> Result<Value, NetworkError> {
    let mut value_length_buf = [0u8; 8];
    if socket.read_exact(&mut value_length_buf).await? != 8 {
        return Err(NetworkError::FailedToReadValue);
    }
    Ok(Value::Integer(i64::from_be_bytes(value_length_buf)))
}

async fn decode_value_string<R: LocalReadAsync>(socket: &mut R) -> Result<Value, NetworkError> {
    let mut value_length_buf = [0u8; 8];
    if socket.read_exact(&mut value_length_buf).await? != 8 {
        return Err(NetworkError::FailedToReadValue);
    }

    // Figure out the size of the string.
    let string_length = u64::from_be_bytes(value_length_buf);

    let mut str_buf = vec![0u8; string_length as usize];
    socket.read_exact(&mut str_buf).await?;

    Ok(Value::String(
        String::from_utf8(str_buf).map_err(|_| NetworkError::FailedToReadValue)?,
    ))
}

pub(crate) async fn read_key<R>(socket: &mut R) -> Result<Key, NetworkError>
where 
    R: LocalReadAsync
{
    // Check the key length, we will read this first before deferring.
    let mut key_buf = [0u8; 4];
    let key_buf_size = socket.read_exact(&mut key_buf).await?;

    if key_buf_size != 4 {
        return Err(NetworkError::FailedToReadKey)?;
    }

    println!("Key buf: {:?}", key_buf);

    // Now we have the key length.
    // We read the key.
    let key_length = u32::from_be_bytes(key_buf) as usize;
    let mut key_bytes = vec![0u8; key_length];
    let read_b = socket.read_exact(&mut key_bytes).await?;
    if read_b != key_length {
        return Err(NetworkError::FailedToReadKey)?;
    }

    // The key text.
    let key_text =
        Key::from_str(std::str::from_utf8(&key_bytes).map_err(|_| NetworkError::FailedToReadKey)?);
    Ok(key_text)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read};

    use crate::{
        access::{WatcherActivity, WatcherBehaviour},
        models::{Key, Value},
        network::{decoder::{
            read_bool, read_optional_value, read_packet, write_bool, write_optional_value,
            write_packet,
        }, PacketId, PacketPayload},
    };

    use super::Packet;

    // use crate::net::{driver::read_packet, Driver};

    #[tokio::test]
    pub async fn read_bool_test() {
        let mut cursor = Cursor::new(vec![0, 1]);
        assert_eq!(read_bool(&mut cursor).await.unwrap(), false);
        assert_eq!(read_bool(&mut cursor).await.unwrap(), true);
    }

    #[tokio::test]
    pub async fn read_optional_value_test() {
        // Write a null.
        let mut cursor = Cursor::new([0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 64, 0].to_vec());
        
        assert_eq!(read_optional_value(&mut cursor).await.unwrap(), None);
        assert_eq!(
            read_optional_value(&mut cursor).await.unwrap(),
            Some(Value::Integer(64))
        );
    }

    #[tokio::test]
    pub async fn write_optional_value_test() {
        // Write a null.
        let mut cursor = vec![];
        write_optional_value(None, &mut cursor).await.unwrap();
        assert_eq!(cursor.len(), 1);
        assert_eq!(cursor[0], 0);

        // Write some value
        let mut cursor = vec![];
        write_optional_value(Some(&Value::Integer(22)), &mut cursor)
            .await
            .unwrap();
        assert_eq!(cursor.len(), 10);
        assert_eq!(i64::from_be_bytes(cursor[2..10].try_into().unwrap()), 22);
    }

    #[tokio::test]
    pub async fn write_bool_test() {
        let mut cursor = vec![];
        write_bool(true, &mut cursor).await.unwrap();
        assert_eq!(cursor[0], 1);
        write_bool(false, &mut cursor).await.unwrap();
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
        buffer.extend_from_slice(&(skey.as_bytes().len() as u32).to_be_bytes());
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
        buffer.extend_from_slice(&(skey.as_bytes().len() as u32).to_be_bytes());
        buffer.extend_from_slice(skey.as_bytes());

        // 1 = Some
        // 1 = Integer
        // 64 0 0 0 0 0 0 0 = A i64 of 64
        // 1 = True
        buffer.extend_from_slice(&[1, 1, 0, 0, 0, 0, 0, 0, 0, 64, 1]);

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
        buffer.extend_from_slice(&(skey.as_bytes().len() as u32).to_be_bytes());
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
        buffer.extend_from_slice(&(skey.as_bytes().len() as u32).to_be_bytes());
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
        buffer.extend_from_slice(&(skey.as_bytes().len() as u32).to_be_bytes());
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
        buffer.extend_from_slice(&(skey.as_bytes().len() as u32).to_be_bytes());
        buffer.extend_from_slice(skey.as_bytes());

        let svalue: i64 = 382;
        buffer.push(1);
        buffer.extend_from_slice(&svalue.to_be_bytes());

        if let PacketPayload::Insert { key, value } = read_packet(&mut Cursor::new(buffer)).await.unwrap().payload()
        {
            assert_eq!(**key, Key::from_str(skey));
            assert_eq!(value.as_integer().unwrap(), svalue);
        } else {
            panic!("Packet did not decode as the proper type.");
        }
    }

    #[tokio::test]
    pub async fn read_string_insert_packet() {
        let skey = "hello";
        let mut buffer = vec![0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0u8];
        buffer.extend_from_slice(&(skey.as_bytes().len() as u32).to_be_bytes());
        buffer.extend_from_slice(skey.as_bytes());

        let svalue = "I am a string to be set.";
        buffer.push(0);
        buffer.extend_from_slice(&(svalue.as_bytes().len() as u64).to_be_bytes());
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
