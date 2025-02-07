
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Key(String);

impl Key {
    pub fn from_str<S: AsRef<str>>(key: S) -> Self {
        Self(key.as_ref().to_string())
    }
}

impl Into<Key> for &str {
    fn into(self) -> Key {
        Key(self.to_string())
    }
}

impl Into<Key> for String {
    fn into(self) -> Key {
        Key(self)
    }
}