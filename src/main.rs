#![allow(unused_imports)]
use std::collections::HashMap;
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
                stream_pool.execute(move || {
                    let fake_db: HashMap<String, String> = HashMap::new();
                    let res = handle_client(_stream, fake_db);
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

fn handle_client(
    mut stream: TcpStream,
    mut fake_db: HashMap<String, String>,
) -> Result<(), Box<dyn Error>> {
    loop {
        let Some(all_lines) = decode_bulk_string(&stream) else {
            break;
        };
        eprintln!("ALL LINES:{:?}", all_lines);

        let cmd = &all_lines[1];

        match cmd.to_lowercase().as_str() {
            "ping" => {
                stream.write_all(b"+PONG\r\n").unwrap();
            }
            "echo" => {
                let resp = [b"+", all_lines[3].as_bytes(), b"\r\n"].concat();
                stream.write_all(&resp).unwrap();
            }
            "set" => {
                let k = all_lines[3].clone();
                let v = all_lines[5].clone();
                eprintln!("inserting k:{k}, v:{v}");
                let _res = fake_db.insert(k, v);

                stream.write_all(b"+OK\r\n").unwrap();
            }
            "get" => {
                eprintln!("in get:{}", all_lines[3]);
                if let Some(res) = fake_db.get(&all_lines[3]) {
                    let res_size = res.len();
                    let resp = [
                        b"$",
                        res_size.to_string().as_bytes(),
                        b"\r\n",
                        res.as_bytes(),
                        b"\r\n",
                    ]
                    .concat();
                    stream.write_all(&resp).unwrap();
                } else {
                    stream.write_all(b"$-1\r\n").unwrap();
                }
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

//https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings
//The exact bytes your program will receive won't be just ECHO hey, you'll receive something like this: *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n. That's ["ECHO", "hey"] encoded using the Redis protocol.
fn decode_bulk_string(stream: &TcpStream) -> Option<Vec<String>> {
    let mut all_lines = Vec::new();
    let mut my_iter = BufReader::new(stream).lines();
    //if next returns None then no more lines, break loop, free thread
    let arr_length = my_iter.next()?;
    //for each element we'll have 2 lines, one with the size and the other with the text
    //so arr_length will ne provided num of elements * 2
    let arr_length = arr_length.unwrap()[1..].parse::<usize>().unwrap() * 2;
    for _ in 0..arr_length {
        all_lines.push(my_iter.next()?.unwrap());
    }
    Some(all_lines)
}
