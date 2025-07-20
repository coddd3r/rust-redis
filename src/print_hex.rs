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
