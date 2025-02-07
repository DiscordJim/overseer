use crate::error::ValueParseError;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Value {
    String(String),
    Integer(i64)
}


impl Value {
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