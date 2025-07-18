#![allow(unused_imports)]
use std::error::Error;
use std::io::{prelude::*, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::usize;

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
    loop {
        let reader = BufReader::new(&stream);
        let mut all_lines = Vec::new();
        let mut my_iter = reader.lines();
        let Some(arr_length) = my_iter.next() else {
            break;
        };
        let arr_length = arr_length.unwrap()[1..].parse::<usize>().unwrap() * 2;
        eprintln!("length: {arr_length}");
        for _ in 0..arr_length {
            all_lines.push(my_iter.next().unwrap().unwrap());
        }
        eprintln!("ALL LINES:{:?}", all_lines);

        let cmd = &all_lines[1];

        match cmd.to_lowercase().as_str() {
            "ping" => {
                stream.write_all(b"+PONG\r\n").unwrap();
            }
            "echo" => {
                let resp = [b"+", all_lines[3].as_bytes(), b"\r\n"].concat();
                stream.write_all(&resp).unwrap()
            }
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
