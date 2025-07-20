use std::fmt;
use std::io;

#[derive(Debug)]
pub enum RdbError {
    Io(io::Error),
    InvalidHeader,
    InvalidVersion,
    UnexpectedEof,
    InvalidSizeEncoding,
    InvalidStringEncoding,
    InvalidValueType(u8),
    ChecksumMismatch,
    UnsupportedFeater(&'static str),
}

impl fmt::Display for RdbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RdbError::Io(e) => write!(f, "IO error: {}", e),
            RdbError::InvalidHeader => write!(f, "Invalid RDB header"),
            RdbError::InvalidVersion => write!(f, "Invalid RDB version"),
            RdbError::UnexpectedEof => write!(f, "Unexpected end of file"),
            RdbError::InvalidSizeEncoding => write!(f, "Invalid size encoding"),
            RdbError::InvalidStringEncoding => write!(f, "Invalid string encoding"),
            RdbError::InvalidValueType(t) => write!(f, "Invalid value type: {}", t),
            RdbError::ChecksumMismatch => write!(f, "Checksum mismatch"),
            RdbError::UnsupportedFeater(feat) => write!(f, "Unsupported feature: {}", feat),
        }
    }
}

impl From<io::Error> for RdbError {
    fn from(err: io::Error) -> Self {
        RdbError::Io(err)
    }
}

pub type Result<T> = std::result::Result<T, RdbError>;
