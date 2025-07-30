use std::fs::File;
use std::io::{Result, Write};
use std::path::Path;

pub fn print_hex_dump(data: &[u8]) {
    const BYTES_PER_LINE: usize = 16;

    for (i, chunk) in data.chunks(BYTES_PER_LINE).enumerate() {
        // Print offset
        print!("{:08x}: ", i * BYTES_PER_LINE);

        // Print hex bytes
        for (j, byte) in chunk.iter().enumerate() {
            print!("{:02x}", byte);
            if j % 2 == 1 {
                print!(" "); // Extra space every 2 bytes
            }
        }

        // Pad line if needed
        if chunk.len() < BYTES_PER_LINE {
            let remaining = BYTES_PER_LINE - chunk.len();
            for ss in 0..remaining {
                print!("  ");
                if (chunk.len() + ss) % 2 == 1 {
                    print!(" ");
                }
            }
        }

        print!(" ");

        // Print ASCII representation
        for byte in chunk {
            if byte.is_ascii_graphic() || *byte == b' ' {
                print!("{}", *byte as char);
            } else {
                print!(".");
            }
        }

        println!();
    }
}

pub fn print_hex(bytes: &[u8]) {
    for byte in bytes {
        print!("{:02x} ", byte);
    }
    println!();
}

pub fn create_dummy_rdb(path: &Path) -> Result<()> {
    let mut file = File::create(path)?;

    // 1. Header section (REDIS0011)
    file.write_all(b"REDIS0011")?;

    // 2. Metadata section
    // redis-ver comes first (length 9)
    file.write_all(&[0xFA])?;
    file.write_all(&[0x09])?; // Length 9
    file.write_all(b"redis-ver")?;
    file.write_all(&[0x05])?; // Length 5
    file.write_all(b"7.2.0")?;

    // redis-bits comes second (length 10)
    file.write_all(&[0xFA])?;
    file.write_all(&[0x0A])?; // Length 10
    file.write_all(b"redis-bits")?;
    file.write_all(&[0xC0, 0x40])?; // 8-bit encoded 64

    // 3. Database section
    file.write_all(&[0xFE, 0x00])?; // DB selector (db 0)
    file.write_all(&[0xFB, 0x01, 0x00])?; // RESIZEDB

    // Key-value pair
    file.write_all(&[0x00])?; // String type
    file.write_all(&[0x03])?; // Length 9
    file.write_all(b"foo")?;
    file.write_all(&[0x03])?; // Length 5
    file.write_all(b"bar")?;

    // 4. EOF section
    file.write_all(&[0xFF])?; // EOF marker
    file.write_all(&[0xCB, 0x2D, 0x78, 0xFA, 0xF9, 0x97, 0x5B, 0xC2])?; // Checksum

    Ok(())
}

/*fn create_dummy_rdb_wrong(path: &Path) -> Result<()> {
    let mut file = File::create(path)?;

    // 1. Header section (REDIS0011)
    file.write_all(b"REDIS0011")?;

    // 2. Metadata section
    // redis-bits comes FIRST (length 10)
    file.write_all(&[0xFA])?;
    file.write_all(&[0x0A])?; // Length 10
    file.write_all(b"redis-bits")?;
    file.write_all(&[0xC0, 0x40])?; // 8-bit encoded 64

    // redis-ver comes SECOND (length 9)
    file.write_all(&[0xFA])?;
    file.write_all(&[0x09])?; // Length 9
    file.write_all(b"redis-ver")?;
    file.write_all(&[0x05])?; // Length 5
    file.write_all(b"7.2.0")?;

    // 3. Database section
    file.write_all(&[0xFE, 0x00])?; // DB selector (db 0)
    file.write_all(&[0xFB, 0x01, 0x00, 0x00])?; // RESIZEDB

    // Key-value pair
    file.write_all(&[0x00])?; // String type
    file.write_all(&[0x09])?; // Length 9
    file.write_all(b"pineapple")?;
    file.write_all(&[0x05])?; // Length 5
    file.write_all(b"apple")?;

    // 4. EOF section
    file.write_all(&[0xFF])?; // EOF marker
    file.write_all(&[0xCB, 0x2D, 0x78, 0xFA, 0xF9, 0x97, 0x5B, 0xC2])?; // Checksum

    Ok(())
}*/
