#![allow(unused_imports)]
use std::error::Error;
use std::io::{prelude::*, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};

//use codecrafters-redis::ThreadPool;
mod threadpool;
use threadpool::ThreadPool;

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
        eprintln!("READ:{read_bytes} bytes");
        if read_bytes == 0 {
            break;
        }
        //decode the command size and message size
        let cmd_size = String::from_utf8_lossy(&[buffer[5]])
            .to_string()
            .parse::<usize>()
            .unwrap();

        let msg_size = String::from_utf8_lossy(&[buffer[6 + cmd_size + 5]])
            .to_string()
            .parse::<usize>()
            .unwrap();

        let command = String::from_utf8_lossy(&buffer[8..8 + cmd_size]).to_string();

        let msg = String::from_utf8_lossy(&buffer[18..18 + msg_size]).to_string();
        eprintln!("msg:{msg}, command:{command}");

        match command.to_lowercase().as_str() {
            "ping" => {
                stream.write_all(b"+PONG\r\n").unwrap();
            }
            "echo" => stream.write_all(msg.as_bytes()).unwrap(),
            i => eprintln!("UNEXPECTED INPUT {i}"),
        }
    }
    Ok(())
}

//https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings
//The exact bytes your program will receive won't be just ECHO hey, you'll receive something like this: *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n. That's ["ECHO", "hey"] encoded using the Redis protocol.
//fn decode_bulk_string(val: &[u8]) -> String {
//    let msg_size = String::from_utf8_lossy(&[val[5]]).to_string();
//    let command = String::from_utf8_lossy(&val[8..12]);
//    return command.to_string();
//}
