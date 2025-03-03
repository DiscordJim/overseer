use thiserror::Error;

#[derive(Error, Debug)]
pub enum ValueParseError {
    #[error("incompatible data type erorr")]
    IncorrectType(String),
}