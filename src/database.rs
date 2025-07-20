/*
*
Here's a more formal description of how each key-value pair is stored:

    Optional expire information (one of the following):
        Timestamp in seconds:
            FD
            Expire timestamp in seconds (4-byte unsigned integer)
        Timestamp in milliseconds:
            FC
            Expire timestamp in milliseconds (8-byte unsigned long)
    Value type (1-byte flag)
    Key (string encoded)
    Value (encoding depends on value type)
* */
use crate::encoding::{read_bytes, read_string, write_bytes, write_string};
use crate::error::{RdbError, Result};
use crate::types::{Expiration, RedisDatabase, RedisValue};
use std::io::{Read, Write};

pub const DB_SELECTOR: u8 = 0xFE;
pub const EXPIRY_SECONDS: u8 = 0xFD;
pub const EXPIRY_MILLISECONDS: u8 = 0xFC;
pub const RESIZEDB: u8 = 0xFB;
pub const EOF: u8 = 0xFF;
pub const STRING_TYPE: u8 = 0x00;

pub fn read_db<R: Read>(reader: &mut R) -> Result<RedisDatabase> {
    let mut db = RedisDatabase::new();

    // Read RESIZEDB info (optional)
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;

    //if we're resizing, skip hash table sizes
    if buf[0] == RESIZEDB {
        eprintln!("IN RESIZE OPERATION, skipping bytes");
        read_bytes(reader)?; // keys size
        read_bytes(reader)?; // expires size
        reader.read_exact(&mut buf)?;
    }

    loop {
        match buf[0] {
            /* The expire timestamp, expressed in Unix time,
            stored as an 4-byte unsigned long, in little-endian (read right-to-left).*/
            /*Timestamp in seconds: 0xFD*/
            EXPIRY_SECONDS => {
                let mut expiry_bytes = [0u8; 4];
                reader.read_exact(&mut expiry_bytes)?;
                let expiry = u32::from_le_bytes(expiry_bytes);
                reader.read_exact(&mut buf)?; // read value type

                let k = read_string(reader)?;
                let v = read_string(reader)?;
                eprintln!("Sec timeout K:{}, v:{}", &k, &v);
                db.insert(
                    k,
                    RedisValue {
                        value: v,
                        expires_at: Some(Expiration::Seconds(expiry)),
                    },
                );
            }
            /*The expire timestamp, expressed in Unix time,
             * stored as an 8-byte unsigned integer, in little-endian (read right-to-left).*/
            /*Timestamp in milliseconds: 0xFC*/
            EXPIRY_MILLISECONDS => {
                let mut expiry_bytes = [0u8; 8];
                reader.read_exact(&mut expiry_bytes)?;
                let expiry = u64::from_le_bytes(expiry_bytes);
                let _ = reader.read_exact(&mut buf);

                let key = read_string(reader)?;
                let value = read_string(reader)?;
                eprintln!("MS timeout K:{}, v:{}", key, value);
                db.insert(
                    key,
                    RedisValue {
                        value,
                        expires_at: Some(Expiration::Milliseconds(expiry)),
                    },
                );
            }
            /* Here, the flag is 0, which means "string without expiry" */
            STRING_TYPE => {
                let key = read_string(reader)?;
                let value = read_string(reader)?;

                eprintln!("STRING no timeout, k:{key}, v:{value}");
                db.insert(
                    key,
                    RedisValue {
                        value,
                        expires_at: None,
                    },
                );
            }
            EOF => break,
            _ => return Err(RdbError::InvalidValueType(buf[0])),
        }
        // Read next byte to determine what comes next
        // if next is an error deal with it
        if let Err(e) = reader.read_exact(&mut buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
            }
            return Err(e.into());
        }
    }

    Ok(db)
}

/// Write database to an RDB file
pub fn write_database<W: Write>(writer: &mut W, db_index: u8, db: &RedisDatabase) -> Result<()> {
    // start with a selector and the provided index
    writer.write_all(&[DB_SELECTOR, db_index])?;

    // Write RESIZEDB info (we don't track sizes, so just write 0)
    writer.write_all(&[RESIZEDB])?;
    write_bytes(writer, &[0])?; // keys size
    write_bytes(writer, &[0])?; // expires size

    for (k, v) in &db.data {
        // if there is an expiry time
        // write expiry
        if let Some(expiry) = &v.expires_at {
            match expiry {
                Expiration::Seconds(seconds) => {
                    writer.write_all(&[EXPIRY_SECONDS])?;
                    writer.write_all(&seconds.to_le_bytes())?;
                }
                Expiration::Milliseconds(milliseconds) => {
                    writer.write_all(&[EXPIRY_MILLISECONDS])?;
                    writer.write_all(&milliseconds.to_le_bytes())?;
                }
            }
        }

        // write string
        writer.write_all(&[STRING_TYPE])?;
        write_string(writer, k)?;
        write_string(writer, &v.value)?;
    }
    Ok(())
}
