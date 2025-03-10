use std::io;

use integer_encoding::VarInt;

use crate::models::{LocalReadAsync, LocalWriteAsync};



pub struct OvrInteger;

impl OvrInteger {
    pub async fn write<VI, W>(data: VI, writer: &mut W) -> std::io::Result<()>
    where 
        VI: VarInt,
        W: LocalWriteAsync
    {
        writer.write_all(data.encode_var_vec()).await?;
        Ok(())

    }
    /// Reads a variable integer.
    pub async fn read<VI, R>(reader: &mut R) -> std::io::Result<VI>
    where 
        VI: VarInt,
        R: LocalReadAsync
    {
        let mut buffer: Vec<u8> = vec![];
        loop {
            let byte = reader.read_u8().await?;
            if byte & 0x80 == 0 {
                buffer.push(byte);
                break;
            } else {
                buffer.push(byte);
            }
        }
        Ok(VI::decode_var(&buffer).ok_or_else(|| std::io::Error::new(io::ErrorKind::InvalidData, "Failed to decode"))?.0)

    }
}





#[cfg(test)]
mod tests {
    use std::{io::Cursor, u16};

    use integer_encoding::{FixedInt, VarInt, VarIntAsyncReader};

    use crate::network::OvrInteger;

   


    #[tokio::test]
    pub async fn test_write_var_int_basic() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        OvrInteger::write(0u64, &mut cursor).await.unwrap();
        cursor.set_position(0);
        assert_eq!(OvrInteger::read::<u64, _>(&mut cursor).await.unwrap(), 0);

        cursor.set_position(0);
        OvrInteger::write(7u64, &mut cursor).await.unwrap();
        cursor.set_position(0);
        assert_eq!(OvrInteger::read::<u64, _>(&mut cursor).await.unwrap(), 7);


        cursor.set_position(0);
        OvrInteger::write(7393i64, &mut cursor).await.unwrap();
        cursor.set_position(0);
        assert_eq!(OvrInteger::read::<i64, _>(&mut cursor).await.unwrap(), 7393);
    }
}