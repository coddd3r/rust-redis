#![allow(unused_imports)]
use std::io::{prelude::*, BufReader, Write};
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

fn handle_client(mut stream: TcpStream) {
    // stream.write_all(b"+PONG\r\n").unwrap();
    let buf_reader = BufReader::new(&stream);
    let _ = buf_reader
        .lines()
        .map(|result| stream.write_all(b"+PONG\r\n").unwrap())
        .take_while(|line| !line.is_empty());
}
