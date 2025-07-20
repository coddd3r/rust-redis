/*
*RDB files begin with a header section, which looks something like this:

52 45 44 49 53 30 30 31 31  // Magic string + version number (ASCII): "REDIS0011".

The header contains the magic string REDIS, followed by a four-character RDB version number. In this challenge, the test RDB files all use version 11. So, the header is always REDIS0011.
*/
use crate::error::{RdbError, Result};
use std::io::{Read, Write};

const RDB_HEADER: &[u8] = b"REDIS";
const RDB_VERSION_LENGTH: usize = 4;

pub fn read_header<R: Read>(reader: &mut R) -> Result<String> {
    let mut header = vec![0u8; RDB_HEADER.len() + RDB_VERSION_LENGTH];
    reader.read_exact(&mut header)?;

    if &header[..RDB_HEADER.len()] != RDB_HEADER {
        return Err(RdbError::InvalidHeader);
    }

    let version = String::from_utf8(header[RDB_HEADER.len()..].to_vec())
        .map_err(|_| RdbError::InvalidVersion)?;
    Ok(version)
}

pub fn write_header<W: Write>(writer: &mut W, version: &str) -> Result<()> {
    if version.len() != RDB_VERSION_LENGTH {
        return Err(RdbError::InvalidVersion);
    }
    let _ = writer.write_all(RDB_HEADER);
    let _ = writer.write_all(version.as_bytes());
    Ok(())
}
