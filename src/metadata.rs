/*
* *Next is the metadata section. It contains zero or more "metadata subsections," which each specify a single metadata attribute. Here's an example of a metadata subsection that specifies redis-ver:
FA                             // Indicates the start of a metadata subsection.
09 72 65 64 69 73 2D 76 65 72  // The name of the metadata attribute (string encoded): "redis-ver".
06 36 2E 30 2E 31 36           // The value of the metadata attribute (string encoded): "6.0.16".

The metadata name and value are always string encoded.

*/
use crate::encoding::{read_string, write_string};
use crate::error::Result;
use std::collections::HashMap;
use std::io::{Read, Write};

const METADATA_START: u8 = 0xFA;

/// Reads metadata section from RDB file
pub fn read_metada<R: Read>(reader: &mut R) -> Result<HashMap<String, String>> {
    eprintln!("READING METADATA HEX");
    let mut metadata = HashMap::new();
    // while there are still metadata sections
    loop {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;

        // reaches section end and return read buf, simulated peek
        if buf[0] != METADATA_START {
            // Not a metadata entry, push the byte back (simulated by peeking)
            eprintln!("before quitting metadata loop:got:{:X?}", buf[0]);
            eprintln!("final metadata: {:?}", &metadata);
            return Ok(metadata);
        }

        let k = read_string(reader)?;
        let v = read_string(reader)?;
        eprintln!("read metadata key:{k}, value{v}");
        metadata.insert(k, v);
    }
}

/// Writes metadata section from RDB file
pub fn write_metadata<W: Write>(writer: &mut W, metadata: &HashMap<String, String>) -> Result<()> {
    eprintln!("Writing metadata with hashmap:{:?}", metadata);
    for (k, v) in metadata {
        writer.write_all(&[METADATA_START])?;
        write_string(writer, k)?;
        write_string(writer, v)?;
    }
    Ok(())
}
