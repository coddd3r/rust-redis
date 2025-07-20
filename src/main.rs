#![allow(unused_imports)]
use std::collections::HashMap;
use std::error::Error;
use std::io::{prelude::*, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::time::Duration;
use std::time::Instant;
use std::usize;
mod threadpool;
use codecrafters_redis::{
    read_rdb_file, write_rdb_file, Expiration, RdbFile, RedisDatabase, RedisValue,
};
use threadpool::ThreadPool;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let arg_list = std::env::args();
    eprintln!("ARGS:{:?}", &arg_list);
    let mut dir = None;
    let mut db_filename = None;
    let mut b = arg_list.into_iter();
    while let Some(a) = b.next() {
        if a.as_str() == "--dir" {
            dir = b.next();
            eprintln!("GOT DIR");
        }
        if a.as_str() == "--dbfilename" {
            db_filename = b.next();
            eprintln!("GOT FILE");
        }
    }

    let stream_pool = ThreadPool::new(4);
    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");
                let dir_arg = dir.clone();
                let db_arg = db_filename.clone();
                stream_pool.execute(move || {
                    let res = handle_client(_stream, dir_arg, db_arg);
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
    dir: Option<String>,
    db_filename: Option<String>,
) -> Result<(), Box<dyn Error>> {
    //let mut fake_db: HashMap<String, (String, Option<Instant>)> = HashMap::new();

    let mut new_db = RedisDatabase::new();
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

                if all_lines.len() > 6 && all_lines[7].to_lowercase() == "px" {
                    let _res = new_db.insert(
                        k,
                        RedisValue {
                            value: v,
                            expires_at: Some(Expiration::Seconds(all_lines[9].parse()?)),
                        },
                    );
                } else {
                    new_db.insert(
                        k,
                        RedisValue {
                            value: v,
                            expires_at: None,
                        },
                    );
                }
                stream.write_all(b"+OK\r\n").unwrap();
            }
            "get" => {
                eprintln!("IN GET");
                if let Some(res) = new_db.get(&all_lines[3]) {
                    if res.expires_at.is_none()
                        || (res.expires_at.is_some()
                            && res.expires_at.as_ref().unwrap().is_expired())
                    {
                        eprintln!("in get TIME STILL");
                        let res_size = res.value.len();
                        let resp = [
                            b"$",
                            res_size.to_string().as_bytes(),
                            b"\r\n",
                            res.value.as_bytes(),
                            b"\r\n",
                        ]
                        .concat();
                        stream.write_all(&resp).unwrap();
                    } else {
                        eprintln!("in get TIME OVER");
                        stream.write_all(b"$-1\r\n").unwrap();
                    }
                } else {
                    eprintln!("IN GET FOUND NOTHING");
                    stream.write_all(b"$-1\r\n").unwrap();
                }
            }
            "config" => {
                let config_command = all_lines[3].to_lowercase();
                let config_field = all_lines[5].to_lowercase();
                match config_command.as_str() {
                    "get" => match config_field.as_str() {
                        "dir" => {
                            let dir_name = dir.as_ref().unwrap();
                            let dir_name_length = dir_name.len().to_string();
                            let resp = [
                                b"*2\r\n$3\r\ndir\r\n$",
                                dir_name_length.as_bytes(),
                                b"\r\n",
                                dir_name.as_bytes(),
                                b"\r\n",
                            ]
                            .concat();
                            stream.write_all(&resp).unwrap();
                        }
                        "dbfilename" => {
                            let db_name = db_filename.as_ref().unwrap();
                            let db_name_length = db_name.len().to_string();
                            let resp = [
                                b"*2\r\n$3\r\ndir\r\n$",
                                db_name_length.as_bytes(),
                                b"\r\n",
                                db_name.as_bytes(),
                                b"\r\n",
                            ]
                            .concat();
                            stream.write_all(&resp).unwrap();
                        }
                        _ => unreachable!(),
                    },
                    _ => {}
                }
            }

            "keys" => {
                if let Some(file) = &db_filename {
                    eprintln!("FOUND FILE");
                    if let Some(directory) = &dir {
                        eprintln!("FOUND DIR");
                        // create a new file path
                        // then write current hashmap to rdb
                        let path = Path::new(directory);
                        let path = path.join(file);
                        eprintln!("USING PATH:{:?}", &path);

                        match read_rdb_file(path) {
                            Ok(rdb) => {
                                eprintln!("Successful rdb read");
                                let mut ret_keys = Vec::new();
                                //get by index
                                if let Some(db) = rdb.databases.get(&0) {
                                    eprintln!("GOT DB FROM RDB FILE");
                                    match all_lines[3].as_str() {
                                        "*" => {
                                            eprintln!("GOT * search");
                                            db.data.iter().for_each(|(k, _)| {
                                                ret_keys.push(k);
                                            });
                                        }
                                        others => {
                                            let search_strings: Vec<&str> =
                                                all_lines[3].split("*").collect();

                                            eprintln!(
                                                "GOT OTHERS search:{others}, searching with {:?}",
                                                search_strings
                                            );
                                            db.data.iter().for_each(|(k, _)| {
                                                if search_strings.iter().all(|e| k.contains(e)) {
                                                    ret_keys.push(k);
                                                }
                                            });
                                        }
                                    }
                                    //EXAMPLE: *1\r\n$3\r\nfoo\r\n
                                    let _ = stream.write_all(
                                        &[b"*", ret_keys.len().to_string().as_bytes()].concat(),
                                    );
                                    ret_keys.iter().enumerate().for_each(|(_, e)| {
                                        //let _ =
                                        //    stream.write_all(&[i.to_string().as_bytes(), b") "].concat());
                                        let _ = stream.write_all(&write_resp_array(e));
                                    });
                                }
                            }
                            Err(e) => {
                                eprintln!("failed to read from rdb file {:?}", e);
                            }
                        }
                    } else {
                        eprintln!("FAILED TO FIND DIR");
                        stream.write_all(b"$-1\r\n").unwrap();
                    }
                } else {
                    eprintln!("FAILED TO FIND FILE");
                    stream.write_all(b"$-1\r\n").unwrap();
                }
            }
            "save" => {
                if let Some(file) = &db_filename {
                    if let Some(directory) = &dir {
                        // create a new file path
                        // then write current hashmap to rdb
                        let mut new_rdb = RdbFile {
                            version: "0011".to_string(),
                            metadata: HashMap::new(),
                            databases: HashMap::new(),
                        };

                        new_rdb.databases.insert(0, new_db.clone());
                        let path = Path::new(&directory);
                        let path = path.join(file);

                        let _ = write_rdb_file(path, &new_rdb);
                    }
                }
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

fn write_resp_array(resp: &String) -> Vec<u8> {
    [
        b"\r\n$",
        resp.len().to_string().as_bytes(),
        b"\r\n",
        resp.as_bytes(),
        b"\r\n",
    ]
    .concat()
}
/**
*
*   https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings
    /The exact bytes your program will receive won't be just ECHO hey, you'll receive something like this: *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n. That's ["ECHO", "hey"] encoded using the Redis protocol.
*
**/
fn decode_bulk_string(stream: &TcpStream) -> Option<Vec<String>> {
    let mut all_lines = Vec::new();
    let mut my_iter = BufReader::new(stream).lines();

    /*
     * if next returns None then no more lines, break loop, free thread
     */
    let arr_length = my_iter.next()?;

    /*
    * for each element we'll have 2 lines, one with the size and the other with the text
        so arr_length will ne provided num of elements * 2
    */
    let arr_length = arr_length.unwrap()[1..].parse::<usize>().unwrap() * 2;
    for _ in 0..arr_length {
        all_lines.push(my_iter.next()?.unwrap());
    }
    Some(all_lines)
}
