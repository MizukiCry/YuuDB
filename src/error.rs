//! A universal error type for the database.

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Error {
    Abort,
    Config(String),
    Internal(String),
    Parse(String),
    ReadOnly,
    Serialization,
    Value(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Abort => write!(f, "Operation aborted"),
            Self::ReadOnly => write!(f, "Read-only transaction"),
            Self::Serialization => write!(f, "Serialization error"),
            Self::Config(s) | Self::Internal(s) | Self::Parse(s) | Self::Value(s) => {
                write!(f, "{}", s)
            }
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Internal(value.to_string())
    }
}
