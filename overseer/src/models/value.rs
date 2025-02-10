use tokio::io::{AsyncRead, AsyncWrite};

use crate::{error::{NetworkError, ValueParseError}, network::decoder::{read_value, write_value}};

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Value {
    String(String),
    Integer(i64)
}


impl Value {
    pub async fn write<W>(&self, writer: &mut W) -> Result<(), NetworkError>
    where 
        W: AsyncWrite + Unpin
    {
        write_value(self, writer).await
    }
    pub async fn read<R>(reader: &mut R) -> Result<Self, NetworkError>
    where 
        R: AsyncRead + Unpin
    {
        read_value(reader).await
    }
    // pub fn from_discriminator(id: u8, bytes: &[u8]) -> Result<Self, NetworkError> {
    //     Ok(match id {
    //         0 => S
    //     })
    // }
    
    pub fn discriminator(&self) -> u8 {
        match self {
            Self::String(..) => 0,
            Self::Integer(..) => 1
        }
    }
    pub fn decode(discrim: u8, bytes: &[u8]) -> Result<Self, NetworkError> {
    
        match discrim {
            0 => Ok(Self::String(std::str::from_utf8(bytes)?.to_string())),
            1 => Ok(Self::Integer(i64::from_le_bytes(bytes.try_into()?))),
            x => Err(NetworkError::UnrecognizedValueTypeDiscriminator(x))
        }
    }
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::String(..) => "string",
            Self::Integer(..) => "integer"
        }
    }
    pub fn as_string(&self) -> Result<&str, ValueParseError> {
        if let Self::String(s) = self {
            Ok(s)
        } else {
            Err(ValueParseError::IncorrectType(format!("Tried to parse as string but was {}.", self.type_name())))
        }
    }
    pub fn as_integer(&self) -> Result<i64, ValueParseError> {
        if let Self::Integer(s) = self {
            Ok(*s)
        } else {
            Err(ValueParseError::IncorrectType(format!("Tried to parse as integer but was {}.", self.type_name())))
        }
    }
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Self::Integer(i) => i.to_le_bytes().to_vec(),
            Self::String(s) => s.as_bytes().to_vec()
        }
    }
}

impl Into<Value> for i64 {
     fn into(self) -> Value {
         Value::Integer(self)
     }
}

impl Into<Value> for &str {
    fn into(self) -> Value {
        Value::String(self.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::Value;


    #[test]
    pub fn test_parse_value() {
        let value = Value::String("hello".to_string());
        assert_eq!(value.as_string().unwrap(), "hello");

        let value = Value::Integer(32);
        assert_eq!(value.as_integer().unwrap(), 32);
    }
}