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
    }
}
impl Packet {
    pub fn discriminator(&self) -> u8 {
        match self {
            Self::Insert { .. } => 0,
            Self::Get { .. } => 1,
            Self::Watch { .. } => 2,
            Self::Release { .. } => 3
        }
    }
}



// Encoder

async fn write_packet<W: AsyncWriteExt + Unpin>(packet: Packet, socket: &mut W) -> Result<(), NetworkError> {
    socket.write_u8(packet.discriminator()).await?;
    match packet {
        Packet::Get {key} => write_get_packet(key, socket).await,
        Packet::Insert { key, value } => write_insert_packet(key, value, socket).await,
        Packet::Release { key } => write_release_packet(key, socket).await,
        Packet::Watch { key, activity, behaviour } => write_watch_packet(key, activity, behaviour, socket).await,
        _ => unreachable!()
    }
}

async fn write_watch_packet<W: AsyncWriteExt + Unpin>(key: Key, activity: WatcherActivity, behaviour: WatcherBehaviour, socket: &mut W) -> Result<(), NetworkError> {
    write_key(&key, socket).await?;
    socket.write_all(&[ activity.discriminator(), behaviour.discriminator() ]).await?;
    Ok(())
}

async fn write_insert_packet<W: AsyncWriteExt + Unpin>(key: Key, value: Value, socket: &mut W) -> Result<(), NetworkError> {
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

async fn write_get_packet<W: AsyncWriteExt + Unpin>(key: Key, socket: &mut W) -> Result<(), NetworkError> {
    write_key(&key, socket).await?;
    Ok(())
}

async fn write_release_packet<W: AsyncWriteExt + Unpin>(key: Key, socket: &mut W) -> Result<(), NetworkError> {
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
        x => Err(NetworkError::UnrecognizedPacketTypeDiscriminator(x))
    }
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
    use std::io::{Cursor, Read};

    use tokio::io::AsyncReadExt;

    use crate::{access::{WatcherActivity, WatcherBehaviour}, models::{Key, Value}, network::decoder::{read_packet, write_packet}};

    use super::Packet;

    // use crate::net::{driver::read_packet, Driver};

    #[tokio::test]
    pub async fn write_watch_packet() {

        let packet = Packet::Watch {
            key: Key::from_str("hello"),
            activity: WatcherActivity::Lazy,
            behaviour: WatcherBehaviour::Eager
        };

        // Write the packet.
        let mut cursor = Cursor::new(vec![]);
        write_packet(packet, &mut cursor).await.unwrap();
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
        write_packet(packet, &mut cursor).await.unwrap();
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
        write_packet(packet, &mut cursor).await.unwrap();
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
        write_packet(packet, &mut cursor).await.unwrap();
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