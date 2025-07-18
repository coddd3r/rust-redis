#![allow(unused_imports)]
use std::error::Error;
use std::io::{prelude::*, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};

use codecrafters_redis::ThreadPool;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let stream_pool = ThreadPool::new(4);
    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");
                stream_pool.execute(|| {
                    let res = handle_client(_stream);
                    match res {
                        Ok(_) => (),
                        Err(e) => eprintln!("Error handling client {}", e),
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    println!("Shutting down.");
}

fn handle_client(mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let mut buffer = [0u8; 512];
    loop {
        let read_bytes = stream.read(&mut buffer).unwrap();
        if read_bytes == 0 {
            break;
        }
        stream.write_all(b"+PONG\r\n").unwrap();
    }
    Ok(())
}
