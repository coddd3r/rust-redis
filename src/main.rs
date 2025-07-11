#![allow(unused_imports)]
use std::io::{prelude::*, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");
                handle_client(_stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(stream: TcpStream) {
    // stream.write_all(b"+PONG\r\n").unwrap();
    let buf_reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    for line in buf_reader.lines() {
        match line {
            Ok(l) => {
                if l.is_empty() {
                    break;
                }
                writer.write_all(b"+PONG\r\n").unwrap();
            }
            Err(e) => eprintln!("{}", e),
        }
    }
}
