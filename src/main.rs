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
    let request: Vec<_> = buf_reader
        .lines()
        //.map(|result| stream.write_all(b"+PONG\r\n").unwrap())
        .map(|result| result.unwrap())
        .take_while(|line| !line.is_empty())
        .collect();
    request
        .iter()
        .for_each(|_| stream.write_all(b"+PONG\r\n").unwrap())
    /*let mut line = String::new();

    loop {
        let line_bytes = buf_reader.read_line(&mut line);
        match line_bytes {
            Ok(0) => break,
            Ok(_) => stream.write_all(b"+PONG\r\n").unwrap(),
            Err(e) => {
                println!("Error reading: {}", e);
                break;
            }
        }
    }*/
}
