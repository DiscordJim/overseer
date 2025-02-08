use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{access::{WatcherActivity, WatcherBehaviour}, error::NetworkError, models::{Key, Value}};

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
    GetReturn {
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
    pub fn watch<K: Into<Key>>(key: K, activity: WatcherActivity, behaviour: WatcherBehaviour) -> Self {
        Self::Watch { key: key.into(), activity, behaviour }
    }
    pub fn insert<K: Into<Key>, V: Into<Value>>(key: K, value: V) -> Self {
        Self::Insert { key: key.into(), value: value.into() }
    }
    pub fn delete<K: Into<Key>>(key: K) -> Self {
        Self::Delete { key: key.into() }
    }
    pub fn get<K: Into<Key>>(key: K) -> Self {
        Self::Get { key: key.into() }
    }
    pub fn discriminator(&self) -> u8 {
        match self {
            Self::Insert { .. } => 0,
            Self::Get { .. } => 1,
            Self::Watch { .. } => 2,
            Self::Release { .. } => 3,
            Self::Delete { .. } => 4,
            Self::Notify { .. } => 5,
            Self::GetReturn { .. } => 6
        }
    }
    pub async fn write<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<(), NetworkError> {
        write_packet(self, writer).await
    }
    pub async fn read<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self, NetworkError> {
        read_packet(reader).await
    }
}



// Encoder

async fn write_packet<W: AsyncWriteExt + Unpin>(packet: &Packet, socket: &mut W) -> Result<(), NetworkError> {
    socket.write_u8(packet.discriminator()).await?;
    match packet {
        Packet::Get {key} => write_get_packet(key, socket).await,
        Packet::Insert { key, value } => write_insert_packet(key, value, socket).await,
        Packet::Release { key } => write_release_packet(key, socket).await,
        Packet::Watch { key, activity, behaviour } => write_watch_packet(key, activity, behaviour, socket).await,
        Packet::Delete { key } => write_delete_packet(key, socket).await,
        Packet::Notify { key, value, more } => write_notify_packet(key, value, *more, socket).await,
        Packet::GetReturn { key, value } => write_getreturn_packet(key, value, socket).await
    }
}

async fn write_getreturn_packet<W: AsyncWriteExt + Unpin>(key: &Key, val: &Option<Value>, socket: &mut W) -> Result<(), NetworkError> {
    write_key(key, socket).await?;
    write_optional_value(val, socket).await?;
    Ok(())
}

async fn write_notify_packet<W: AsyncWriteExt + Unpin>(key: &Key, value: &Option<Value>, more: bool, socket: &mut W) -> Result<(), NetworkError> {
    write_key(&key, socket).await?;
    write_optional_value(value, socket).await?;
    write_bool(more, socket).await?;
    Ok(())
}

async fn write_bool<W: AsyncWriteExt + Unpin>(val: bool, writer: &mut W) -> Result<(), NetworkError> {
    if val {
        writer.write_all(&[1]).await?;
    } else {
        writer.write_all(&[0]).await?;
    }
    Ok(())
}

async fn read_bool<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<bool, NetworkError> {
    Ok(match reader.read_u8().await? {
        0 => false,
        1 => true,
        _ => Err(NetworkError::ErrorDecodingBoolean)?
    })
}

async fn write_optional_value<W: AsyncWriteExt + Unpin>(val: &Option<Value>, writer: &mut W) -> Result<(), NetworkError> {
    match val {
        Some(v) => {
            writer.write_all(&[1]).await?;
            write_value(v, writer).await?;
        },
        None => {
            writer.write_all(&[0]).await?;
        }
    }
    Ok(())
}

async fn read_optional_value<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Option<Value>, NetworkError> {
    Ok(match reader.read_u8().await? {
        0 => None,
        1 => Some(decode_value(reader).await?),
        _ => Err(NetworkError::ErrorDecodingOption)?
    })
}

async fn write_watch_packet<W: AsyncWriteExt + Unpin>(key: &Key, activity: &WatcherActivity, behaviour:& WatcherBehaviour, socket: &mut W) -> Result<(), NetworkError> {
    write_key(&key, socket).await?;
    socket.write_all(&[ activity.discriminator(), behaviour.discriminator() ]).await?;
    Ok(())
}

async fn write_insert_packet<W: AsyncWriteExt + Unpin>(key: &Key, value: &Value, socket: &mut W) -> Result<(), NetworkError> {
    write_key(&key, socket).await?;
    write_value(&value, socket).await?;
    Ok(())
}

async fn write_value<W: AsyncWriteExt + Unpin>(value: &Value, socket: &mut W) -> Result<(), NetworkError> {
    match value {
        Value::String(s) => write_value_string(s, socket).await,
        Value::Integer(s) => write_value_integer(*s, socket).await
    }
}

async fn write_value_string<W: AsyncWriteExt + Unpin>(value: &str, socket: &mut W) -> Result<(), NetworkError> {
    socket.write_all(&[0]).await?;
    socket.write_all(&(value.len() as u64).to_le_bytes()).await?;
    socket.write_all(value.as_bytes()).await?;
    Ok(())
}

async fn write_value_integer<W: AsyncWriteExt + Unpin>(value: i64, socket: &mut W) -> Result<(), NetworkError> {
    socket.write_all(&[1]).await?;
    socket.write_all(&value.to_le_bytes()).await?;
    Ok(())
}

async fn write_delete_packet<W: AsyncWriteExt + Unpin>(key: &Key, socket: &mut W) -> Result<(), NetworkError> {
    write_key(&key, socket).await?;
    Ok(())
}

async fn write_get_packet<W: AsyncWriteExt + Unpin>(key: &Key, socket: &mut W) -> Result<(), NetworkError> {
    write_key(&key, socket).await?;
    Ok(())
}

async fn write_release_packet<W: AsyncWriteExt + Unpin>(key: &Key, socket: &mut W) -> Result<(), NetworkError> {
    write_key(&key, socket).await?;
    Ok(())
}

async fn write_key<W: AsyncWriteExt + Unpin>(key: &Key, socket: &mut W) -> Result<(), NetworkError> {
    socket.write_all(&(key.as_str().len() as u32).to_le_bytes()).await?;
    socket.write_all(key.as_str().as_bytes()).await?;
    Ok(())
}

// Decoder

/// Reads a packet by deferring to submethods.
async fn read_packet<R: AsyncRead + Unpin>(socket: &mut R) -> Result<Packet, NetworkError> {
    let discrim = socket.read_u8().await?;
    match discrim {
        0 => read_set_packet(socket).await,
        1 => read_get_packet(socket).await,
        2 => read_watch_packet(socket).await,
        3 => read_release_packet(socket).await,
        4 => read_delete_packet(socket).await,
        5 => read_notify_packet(socket).await,
        6 => read_getreturn_packet(socket).await,
        x => Err(NetworkError::UnrecognizedPacketTypeDiscriminator(x))
    }
}


/// Reads a packet of the set type.
async fn read_getreturn_packet<R: AsyncRead + Unpin>(socket: &mut R) -> Result<Packet, NetworkError> {
    let key = read_key(socket).await?;
    let value = read_optional_value(socket).await?;
    Ok(Packet::GetReturn { key, value })
}
/// Reads a packet of the set type.
async fn read_notify_packet<R: AsyncRead + Unpin>(socket: &mut R) -> Result<Packet, NetworkError> {
    let key = read_key(socket).await?;
    let value = read_optional_value(socket).await?;
    let more = read_bool(socket).await?;
    Ok(Packet::Notify { key, value, more })
}

/// Reads a packet of the set type.
async fn read_delete_packet<R: AsyncRead + Unpin>(socket: &mut R) -> Result<Packet, NetworkError> {
    let key = read_key(socket).await?;
    Ok(Packet::Delete { key })
}

/// Reads a packet of the set type.
async fn read_release_packet<R: AsyncRead + Unpin>(socket: &mut R) -> Result<Packet, NetworkError> {
    let key = read_key(socket).await?;
    Ok(Packet::Release { key })
}

/// Reads a packet of the set type.
async fn read_get_packet<R: AsyncRead + Unpin>(socket: &mut R) -> Result<Packet, NetworkError> {
    let key = read_key(socket).await?;
    Ok(Packet::Get { key })
}

/// Reads a packet of the set type.
async fn read_set_packet<R: AsyncRead + Unpin>(socket: &mut R) -> Result<Packet, NetworkError> {
    let key = read_key(socket).await?;
    let value = decode_value(socket).await?;
    Ok(Packet::Insert { key, value })
}

/// Reads a packet of the set type.
async fn read_watch_packet<R: AsyncRead + Unpin>(socket: &mut R) -> Result<Packet, NetworkError> {
    let key = read_key(socket).await?;
    let activity = WatcherActivity::try_from(socket.read_u8().await?)?;
    let behaviour = WatcherBehaviour::try_from(socket.read_u8().await?)?;
    Ok(Packet::Watch { key, activity, behaviour })
}


async fn decode_value<R: AsyncRead + Unpin>(socket: &mut R) -> Result<Value, NetworkError> {
    let type_discrim = socket.read_u8().await?;
    match type_discrim {
        0 => decode_value_string(socket).await,
        1 => decode_value_integer(socket).await,
        x => Err(NetworkError::UnrecognizedValueTypeDiscriminator(x))
    }
}

async fn decode_value_integer<R: AsyncRead + Unpin>(socket: &mut R) -> Result<Value, NetworkError> {
    let value_length_buf = &mut [0u8; 8];
    if socket.read_exact(value_length_buf).await? != 8 {
        return Err(NetworkError::FailedToReadValue);
    }
    Ok(Value::Integer(i64::from_le_bytes(*value_length_buf)))
}

async fn decode_value_string<R: AsyncRead + Unpin>(socket: &mut R) -> Result<Value, NetworkError> {
    let value_length_buf = &mut [0u8; 8];
    if socket.read_exact(value_length_buf).await? != 8 {
        return Err(NetworkError::FailedToReadValue);
    }
    
    // Figure out the size of the string.
    let string_length = u64::from_le_bytes(*value_length_buf);

    let mut str_buf = vec![0u8; string_length as usize];
    socket.read_exact(&mut str_buf).await?;

    Ok(Value::String(String::from_utf8(str_buf).map_err(|_| NetworkError::FailedToReadValue)?))
    



}


async fn read_key<R: AsyncRead + Unpin>(socket: &mut R) -> Result<Key, NetworkError> {
    // Check the key length, we will read this first before deferring.
    let key_buf = &mut [0u8; 4];
    let key_buf_size = socket.read_exact(key_buf).await?;

    if key_buf_size != 4 {
        return Err(NetworkError::FailedToReadKey)?;
    }


    // Now we have the key length.
    // We read the key.
    let key_length = u32::from_le_bytes(*key_buf) as usize;
    let key_bytes = &mut vec![0u8; key_length];
    let read_b = socket.read(key_bytes).await?;
    if read_b != key_length {
        return Err(NetworkError::FailedToReadKey)?;
    }


    // The key text.
    let key_text = Key::from_str(std::str::from_utf8(key_bytes).map_err(|_| NetworkError::FailedToReadKey)?);
    Ok(key_text)

}



#[cfg(test)]
mod tests {
    use std::io::Cursor;


    use crate::{access::{WatcherActivity, WatcherBehaviour}, models::{Key, Value}, network::decoder::{read_bool, read_optional_value, read_packet, write_bool, write_optional_value, write_packet}};

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
        let mut cursor = Cursor::new([0, 1, 1, 64, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(read_optional_value(&mut cursor).await.unwrap(), None);
        assert_eq!(read_optional_value(&mut cursor).await.unwrap(), Some(Value::Integer(64)));


    }

    #[tokio::test]
    pub async fn write_optional_value_test() {
        // Write a null.
        let mut cursor = vec![];
        write_optional_value(&None, &mut cursor).await.unwrap();
        assert_eq!(cursor.len(), 1);
        assert_eq!(cursor[0], 0);

        // Write some value
        let mut cursor = vec![];
        write_optional_value(&Some(Value::Integer(22)), &mut cursor).await.unwrap();
        assert_eq!(cursor.len(), 10);
        assert_eq!(i64::from_le_bytes(cursor[2..10].try_into().unwrap()), 22);
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

        let packet = Packet::Notify {
            key: Key::from_str("hello"),
            value: None,
            more: false
        };

        // Write the packet.
        let mut cursor = Cursor::new(vec![]);
        packet.write(&mut cursor).await.unwrap();
        cursor.set_position(0);

        

        if let Packet::Notify { key, value, more } = read_packet(&mut cursor).await.unwrap() {
            assert_eq!(key.as_str(), "hello");
            assert!(value.is_none());
            assert!(!more);
        } else {
            panic!("Wrong packet type.");
        }
    }

    #[tokio::test]
    pub async fn write_delete_packet() {

        let packet = Packet::Delete {
            key: Key::from_str("hello"),
        };

        // Write the packet.
        let mut cursor = Cursor::new(vec![]);
        packet.write(&mut cursor).await.unwrap();
        cursor.set_position(0);

        

        if let Packet::Delete { key } = read_packet(&mut cursor).await.unwrap() {
            assert_eq!(key.as_str(), "hello");
        } else {
            panic!("Wrong packet type.");
        }
    }

    #[tokio::test]
    pub async fn write_release_packet() {

        let packet = Packet::Release {
            key: Key::from_str("hello"),
        };

        // Write the packet.
        let mut cursor = Cursor::new(vec![]);
        packet.write(&mut cursor).await.unwrap();
        cursor.set_position(0);

        

        if let Packet::Release { key } = read_packet(&mut cursor).await.unwrap() {
            assert_eq!(key.as_str(), "hello");
        } else {
            panic!("Wrong packet type.");
        }
    }

    #[tokio::test]
    pub async fn write_watch_packet() {

        let packet = Packet::Watch {
            key: Key::from_str("hello"),
            activity: WatcherActivity::Lazy,
            behaviour: WatcherBehaviour::Eager
        };

        // Write the packet.
        let mut cursor = Cursor::new(vec![]);
        write_packet(&packet, &mut cursor).await.unwrap();
        cursor.set_position(0);

        

        if let Packet::Watch { key, activity, behaviour } = read_packet(&mut cursor).await.unwrap() {
            assert_eq!(key.as_str(), "hello");
            assert_eq!(activity, WatcherActivity::Lazy);
            assert_eq!(behaviour, WatcherBehaviour::Eager);
        } else {
            panic!("Wrong packet type.");
        }
    }

    #[tokio::test]
    pub async fn write_insert_string_packet() {

        let packet = Packet::Insert { key: "hello".into(), value: Value::String("hello world".to_string()) };

        // Write the packet.
        let mut cursor = Cursor::new(vec![]);
        write_packet(&packet, &mut cursor).await.unwrap();
        cursor.set_position(0);

        

        if let Packet::Insert { key, value } = read_packet(&mut cursor).await.unwrap() {
            assert_eq!(key.as_str(), "hello");
            assert_eq!(value.as_string().unwrap(), "hello world");
        } else {
            panic!("Wrong packet type.");
        }
    }

    #[tokio::test]
    pub async fn write_insert_integer_packet() {

        let packet = Packet::Insert { key: "hello".into(), value: Value::Integer(32) };

        // Write the packet.
        let mut cursor = Cursor::new(vec![]);
        write_packet(&packet, &mut cursor).await.unwrap();
        cursor.set_position(0);

        if let Packet::Insert { key, value } = read_packet(&mut cursor).await.unwrap() {
            assert_eq!(key.as_str(), "hello");
            assert_eq!(value.as_integer().unwrap(), 32);
        } else {
            panic!("Wrong packet type.");
        }
    }

    #[tokio::test]
    pub async fn write_get_packet() {

        let packet = Packet::Get { key: "hello".into() };

        // Write the packet.
        let mut cursor = Cursor::new(vec![]);
        write_packet(&packet, &mut cursor).await.unwrap();
        cursor.set_position(0);

        if let Packet::Get { key } = read_packet(&mut cursor).await.unwrap() {
            assert_eq!(key.as_str(), "hello");
        } else {
            panic!("Wrong packet type.");
        }
    }

    #[tokio::test]
    pub async fn read_release_packet() {

        let skey = "hello";
        let mut buffer = vec![3u8];
        buffer.extend_from_slice(&(skey.as_bytes().len() as u32).to_le_bytes());
        buffer.extend_from_slice(skey.as_bytes());

   

        if let Packet::Release { key } = read_packet(&mut Cursor::new(buffer)).await.unwrap() {
            assert_eq!(key, Key::from_str(skey));
        } else {
            panic!("Packet did not decode as the proper type.");
        }
    }

    #[tokio::test]
    pub async fn read_notify_packet() {

        let skey = "hello";
        let mut buffer = vec![5u8];
        buffer.extend_from_slice(&(skey.as_bytes().len() as u32).to_le_bytes());
        buffer.extend_from_slice(skey.as_bytes());

        // 1 = Some
        // 1 = Integer
        // 64 0 0 0 0 0 0 0 = A i64 of 64
        // 1 = True
        buffer.extend_from_slice(&[ 1, 1, 64, 0, 0, 0, 0, 0, 0, 0, 1 ]);


        if let Packet::Notify { key, value, more } = Packet::read(&mut Cursor::new(buffer)).await.unwrap() {
            assert_eq!(key, Key::from_str(skey));
            assert_eq!(value.unwrap(), Value::Integer(64));
            assert!(more);
        } else {
            panic!("Packet did not decode as the proper type.");
        }
    }

    #[tokio::test]
    pub async fn read_watch_packet() {

        let skey = "hello";
        let mut buffer = vec![2u8];
        buffer.extend_from_slice(&(skey.as_bytes().len() as u32).to_le_bytes());
        buffer.extend_from_slice(skey.as_bytes());

        buffer.push(1);
        buffer.push(0);

        if let Packet::Watch { key, activity, behaviour } = read_packet(&mut Cursor::new(buffer)).await.unwrap() {
            assert_eq!(key, Key::from_str(skey));
            assert_eq!(activity, WatcherActivity::Lazy);
            assert_eq!(behaviour, WatcherBehaviour::Ordered);
        } else {
            panic!("Packet did not decode as the proper type.");
        }
    }

    #[tokio::test]
    pub async fn read_delete_packet() {

        let skey = "hello";
        let mut buffer = vec![4u8];
        buffer.extend_from_slice(&(skey.as_bytes().len() as u32).to_le_bytes());
        buffer.extend_from_slice(skey.as_bytes());

        if let Packet::Delete { key } = Packet::read(&mut Cursor::new(buffer)).await.unwrap() {
            assert_eq!(key, Key::from_str(skey));
        } else {
            panic!("Packet did not decode as the proper type.");
        }
    }


    #[tokio::test]
    pub async fn read_get_packet() {

        let skey = "hello";
        let mut buffer = vec![1u8];
        buffer.extend_from_slice(&(skey.as_bytes().len() as u32).to_le_bytes());
        buffer.extend_from_slice(skey.as_bytes());

        if let Packet::Get { key } = read_packet(&mut Cursor::new(buffer)).await.unwrap() {
            assert_eq!(key, Key::from_str(skey));
        } else {
            panic!("Packet did not decode as the proper type.");
        }
    }

    #[tokio::test]
    pub async fn read_integer_insert_packet() {

        let skey = "hello";
        let mut buffer = vec![0u8];
        buffer.extend_from_slice(&(skey.as_bytes().len() as u32).to_le_bytes());
        buffer.extend_from_slice(skey.as_bytes());

        let svalue: i64 = 382;
        buffer.push(1);
        buffer.extend_from_slice(&svalue.to_le_bytes());


        if let Packet::Insert { key, value } = read_packet(&mut Cursor::new(buffer)).await.unwrap() {
            assert_eq!(key, Key::from_str(skey));
            assert_eq!(value.as_integer().unwrap(), svalue);
        } else {
            panic!("Packet did not decode as the proper type.");
        }
    }


    #[tokio::test]
    pub async fn read_string_insert_packet() {

        let skey = "hello";
        let mut buffer = vec![0u8];
        buffer.extend_from_slice(&(skey.as_bytes().len() as u32).to_le_bytes());
        buffer.extend_from_slice(skey.as_bytes());

        let svalue = "I am a string to be set.";
        buffer.push(0);
        buffer.extend_from_slice(&(svalue.as_bytes().len() as u64).to_le_bytes());
        buffer.extend_from_slice(svalue.as_bytes());


        if let Packet::Insert { key, value } = read_packet(&mut Cursor::new(buffer)).await.unwrap() {
            assert_eq!(key, Key::from_str(skey));
            assert_eq!(value.as_string().unwrap(), svalue);
        } else {
            panic!("Packet did not decode as the proper type.");
        }
    }
}