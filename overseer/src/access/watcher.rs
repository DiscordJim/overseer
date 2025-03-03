use crate::error::NetworkError;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum WatcherBehaviour {
    /// Watcher returns values in order
    Ordered,
    /// The watcher only stores the immediate result.
    Eager
}

impl TryFrom<u8> for WatcherBehaviour {
    type Error = NetworkError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => Self::Ordered,
            1 => Self::Eager,
            _ => Err(NetworkError::WatcherBehaviourDecodeError)?
        })
    }
}

impl TryFrom<u8> for WatcherActivity {
    type Error = NetworkError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => Self::Kickback,
            1 => Self::Lazy,
            _ => Err(NetworkError::WatcherActivityDecodeError)?
        })
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum WatcherActivity {
    /// Kicks the initial state back to the watcher immediately.
    Kickback,
    /// The watcher looks for new updates only.
    Lazy
}

impl WatcherActivity {
    pub fn discriminator(&self) -> u8 {
        match self {
            Self::Kickback => 0,
            Self::Lazy => 1
        }
    }
}

impl WatcherBehaviour {
    pub fn discriminator(&self) -> u8 {
        match self {
            Self::Ordered => 0,
            Self::Eager => 1
        }
    }
}