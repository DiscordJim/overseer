
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Key(String);

impl Key {
    pub fn from_str<S: AsRef<str>>(key: S) -> Self {
        Self(key.as_ref().to_string())
    }
}