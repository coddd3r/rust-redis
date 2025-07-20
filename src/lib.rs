mod database;
mod encoding;
mod error;
mod header;
mod metadata;
mod types;

pub use error::{RdbError, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
pub use types::{Expiration, RdbFile, RedisDatabase, RedisValue};

/// Reads an RDB file from disk
pub fn read_rdb_file<P: AsRef<Path>>(path: P) -> Result<RdbFile> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    read_rdb(&mut reader)
}

/// Reads RDB data from a reader
pub fn read_rdb<R: Read>(reader: &mut R) -> Result<RdbFile> {
    let version = header::read_header(reader)?;
    let metadata = metadata::read_metada(reader)?;
    let mut all_databases = HashMap::new();

    // read while there are databse subsections(which are dbs themselves)
    loop {
        let mut buf = [0u8; 1];
        //if no more bytes, EOF reached
        if reader.read_exact(&mut buf).is_err() {
            break;
        }

        match buf[0] {
            // if a db subsection is found
            database::DB_SELECTOR => {
                // Read database index, 1 byte
                let mut db_index_buf = [0u8; 1];
                reader.read_exact(&mut db_index_buf)?;
                let db_index = db_index_buf[0];

                let db = database::read_db(reader)?;
                all_databases.insert(db_index, db);
            }

            database::EOF => break,
            // anything else is invalid
            _ => return Err(RdbError::InvalidValueType(buf[0])),
        }
    }
    Ok(RdbFile {
        version,
        metadata,
        databases: all_databases,
    })
}

/**
* WRITE
**/
/// Writes an RDB file to disk
pub fn write_rdb_file<P: AsRef<Path>>(path: P, rdb: &RdbFile) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    write_rdb(&mut writer, rdb)
}

/// Writes RDB data to a writer
pub fn write_rdb<W: Write>(writer: &mut W, rdb: &RdbFile) -> Result<()> {
    // Write header
    header::write_header(writer, &rdb.version)?;

    // Write metadata
    metadata::write_metadata(writer, &rdb.metadata)?;

    // Write databases
    for (db_index, db) in &rdb.databases {
        database::write_database(writer, *db_index, db)?;
    }

    // Write EOF marker
    writer.write_all(&[database::EOF])?;

    Ok(())
}
