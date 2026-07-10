//! Error types for the IoTDB client.

use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    /// Underlying Thrift transport/protocol failure.
    Thrift(thrift::Error),
    /// Non-success status code returned by the server (TSStatus).
    Server { code: i32, message: String },
    /// Client-side usage or state error (e.g. session not open).
    Client(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Thrift(e) => write!(f, "thrift error: {e}"),
            Error::Server { code, message } => write!(f, "server error {code}: {message}"),
            Error::Client(msg) => write!(f, "client error: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<thrift::Error> for Error {
    fn from(e: thrift::Error) -> Self {
        Error::Thrift(e)
    }
}
