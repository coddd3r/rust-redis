/*
Size encoding

Size-encoded values specify the size of something. Here are some examples:

    The database indexes and hash table sizes are size encoded.
    String encoding begins with a size-encoded value that specifies the number of characters in the string.
    List encoding begins with a size-encoded value that specifies the number of elements in the list.

The first (most significant) two bits of a size-encoded value indicate how the value should be parsed. Here's a guide (bits are shown in both hexadecimal and binary):

/* If the first two bits are 0b00:
   The size is the remaining 6 bits of the byte.
   In this example, the size is 10: */
0A
00001010

/* If the first two bits are 0b01:
   The size is the next 14 bits
   (remaining 6 bits in the first byte, combined with the next byte),
   in big-endian (read left-to-right).
   In this example, the size is 700: */
42 BC
01000010 10111100

/* If the first two bits are 0b10:
   Ignore the remaining 6 bits of the first byte.
   The size is the next 4 bytes, in big-endian (read left-to-right).
   In this example, the size is 17000: */
80 00 00 42 68
10000000 00000000 00000000 01000010 01101000

/* If the first two bits are 0b11:
   The remaining 6 bits specify a type of string encoding.
   See string encoding section. */

String encoding

A string-encoded value consists of two parts:

    The size of the string (size encoded).
    The string.

Here's an example:

/* The 0x0D size specifies that the string is 13 characters long.
   The remaining characters spell out "Hello, World!". */
0D 48 65 6C 6C 6F 2C 20 57 6F 72 6C 64 21

For sizes that begin with 0b11, the remaining 6 bits indicate a type of string format:

/* The 0xC0 size indicates the string is an 8-bit integer.
   In this example, the string is "123". */
C0 7B

/* The 0xC1 size indicates the string is a 16-bit integer.
   The remaining bytes are in little-endian (read right-to-left).
   In this example, the string is "12345". */
C1 39 30

/* The 0xC2 size indicates the string is a 32-bit integer.
   The remaining bytes are in little-endian (read right-to-left),
   In this example, the string is "1234567". */
C2 87 D6 12 00

/* The 0xC3 size indicates that the string is compressed with the LZF algorithm.
   You will not encounter LZF-compressed strings in this challenge. */
C3 ...
* */

use crate::print_hex::print_hex;

use super::error::{RdbError, Result};
use std::io::{Read, Write};

/// Reads a size-encoded value from the stream
pub fn read_size<R: Read>(reader: &mut R) -> Result<(usize, Option<Vec<u8>>)> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    let first_byte = buf[0];

    //eprintln!("reading first byte for SIZE in hex {:#04X?}", buf[0]);
    //read first byte by right shifting by 6
    match first_byte >> 6 {
        /* If the first two bits are 0b00:
        The size is the remaining 6 bits of the byte.
        In this example, the size is 10: */
        0b00 => Ok(((first_byte & 0b00111111) as usize, None)),

        /* If the first two bits are 0b01:
           The size is the next 14 bits
           (remaining 6 bits in the first byte, combined with the next byte),
           in big-endian (read left-to-right).
           In this example, the size is 700:
        42 BC
        01000010 10111100 */
        0b01 => {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf)?;
            let second_byte = buf[0];
            Ok((
                ((first_byte & 0b00111111) as usize) << 8 | second_byte as usize,
                None,
            ))
        }

        /* If the first two bits are 0b10:
           Ignore the remaining 6 bits of the first byte.00000040: 2d78 faf9 975b c2                        -x...[.
           The size is the next 4 bytes, in big-endian (read left-to-right).
           In this example, the size is 17000:
        80 00 00 42 68
        10000000 00000000 00000000 01000010 01101000 */
        0b10 => {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            Ok((u32::from_be_bytes(buf) as usize, None))
        }

        /* If the first two bits are 0b11:
        The remaining 6 bits specify a type of string encoding.
        See string encoding section. */
        0b11 => {
            let mut v = Vec::new();
            v.push(buf[0]);
            Ok((usize::MAX, Some(v)))
        }
        _ => unreachable!(),
    }
}

pub fn write_size<W: Write>(writer: &mut W, size: usize) -> Result<()> {
    if size < 0b01000000 {
        // 6 bits
        writer.write_all(&[size as u8])?;
    } else if size < 0b0100000000000000 {
        // 14 bits
        let bytes = [((size >> 8) as u8 | 0b01000000), (size & 0xFF) as u8];
        writer.write_all(&bytes)?;
    } else if size < u32::MAX as usize {
        // 32 bits
        let mut bytes = [0u8; 5];
        bytes[0] = 0b10000000;
        bytes[1..5].copy_from_slice(&(size as u32).to_be_bytes());
        writer.write_all(&bytes)?;
    } else {
        return Err(RdbError::InvalidSizeEncoding);
    }
    Ok(())
}

pub fn read_string<R: Read>(reader: &mut R) -> Result<String> {
    let res = read_size(reader)?;
    let size = res.0;
    if size == usize::MAX {
        return read_special_int(reader, res.1);
    }
    let mut buf = vec![0u8; size];
    reader.read_exact(&mut buf)?;
    //eprintln!("READ SIZE of a STRING, {size}");
    print!("STRING IN HEX:");
    print_hex(&buf);
    String::from_utf8(buf).map_err(|_| RdbError::InvalidStringEncoding)
}

pub fn write_string<W: Write>(writer: &mut W, s: &str) -> Result<()> {
    write_size(writer, s.len())?;
    writer.write_all(s.as_bytes())?;
    Ok(())
}

/// Reads a length-prefixed byte array
//pub fn read_bytes<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
//    //eprintln!("READING SIZE of a byte array");
//    let size = read_size(reader)?.0;
//    let mut buf = vec![0u8; size];
//    reader.read_exact(&mut buf)?;
//    Ok(buf)
//}

pub fn read_special_int<R: Read>(reader: &mut R, v: Option<Vec<u8>>) -> Result<String> {
    let mut buf = [0u8; 1];
    // reader.read_exact(&mut buf)?;

    let use_vec = v.unwrap()[0];
    //eprintln!("reading special int:{:#04X?}", use_vec);
    match use_vec {
        0xC0 => {
            // 8-bit
            reader.read_exact(&mut buf)?;
            let ret = buf[0].to_string();
            //eprintln!("returning string from special int: {ret}");
            Ok(ret)
        }
        0xC1 => {
            // 16-bit
            let mut bytes = [0u8; 2];
            reader.read_exact(&mut bytes)?;
            let ret = u16::from_le_bytes(bytes).to_string();
            //eprintln!("returning string from special int: {ret}");
            Ok(ret)
        }
        0xC2 => {
            // 32-bit
            let mut bytes = [0u8; 4];
            reader.read_exact(&mut bytes)?;
            let ret = u32::from_le_bytes(bytes).to_string();
            //eprintln!("returning string from special int: {ret}");
            Ok(ret)
        }
        _ => {
            //eprintln!("FAILED special int, with buf:{:#04X?}", buf[0]);
            Err(RdbError::InvalidStringEncoding)
        }
    }
}
