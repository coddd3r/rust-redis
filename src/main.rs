#![allow(unused_imports)]
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{prelude::*, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{env, usize};

use codecrafters_redis::{
    print_hex, read_rdb_file, write_rdb_file, Expiration, RdbError, RdbFile, RedisDatabase,
    RedisValue,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
mod threadpool;
use threadpool::ThreadPool;
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum Role {
    Master(String),
    Slave(String),
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum ReplicationValues {
    Role(Role),
    MasterRelpOffset(usize),
    MasterReplid(String),
}

//struct InfoFieldS {
//    role: Role,
//    master_repl_offset: usize,
//    master_repl_id: String,
//}

const ROLE: &str = "role";
const MASTER: &str = "master";
const SLAVE: &str = "slave";
const MASTER_REPL_OFFSET: &str = "master_repl_offset";
const MASTER_REPL_ID: &str = "master_repl_id";

fn main() {
    let mut id = Uuid::new_v4().to_string();
    id.push_str("2025");
    let mut info_fields: HashMap<&str, String> = HashMap::new();
    info_fields.insert(ROLE, MASTER.to_string());
    eprintln!("ID:{:?}", (&id).len());
    //info_fields.insert("id", id);

    let arg_list = std::env::args();
    //eprintln!("ARGS:{:?}", &arg_list);
    let mut dir = None;
    let mut db_filename = None;
    let mut port = None;
    let mut b = arg_list.into_iter();
    while let Some(a) = b.next() {
        match a.as_str() {
            "--dir" => {
                dir = b.next();
                //eprintln!("GOT DIR");
            }
            "--dbfilename" => {
                db_filename = b.next();
                //eprintln!("GOT ILE");
            }
            "--port" => port = b.next(),
            "--replicaof" => {
                let curr_role = info_fields.get_mut(ROLE).unwrap();
                *curr_role = SLAVE.to_string();
                if let Some(master) = b.next() {
                    eprintln!("is replica of:{master}");
                }
                /*
                master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
                master_repl_offset:0
                */
            }
            _ => {}
        }
    }

    let rl = info_fields.get(ROLE);
    if let Some(r) = rl {
        if r == MASTER {
            info_fields.insert(MASTER_REPL_ID, id.clone());
            info_fields.insert(MASTER_REPL_OFFSET, "0".to_string());
        }
    }

    let mut full_port = String::from("127.0.0.1:");
    if let Some(p) = port {
        full_port.push_str(&p);
    } else {
        full_port.push_str("6379");
    }

    eprintln!("Binding to port:{full_port}");
    let listener = TcpListener::bind(&full_port).unwrap();
    eprintln!("Listening on port:{full_port}");

    let stream_pool = ThreadPool::new(4);
    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");
                let dir_arg = dir.clone();
                let db_arg = db_filename.clone();
                let info_fields = info_fields.clone();
                stream_pool.execute(move || {
                    let res = handle_client(_stream, dir_arg, db_arg, &info_fields);
                    match res {
                        Ok(_) => (),
                        Err(e) => {
                            eprintln!("Error handling client {}", e);
                        }
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
    info_fields: &HashMap<&str, String>,
) -> Result<(), Box<dyn Error>> {
    const RESP_OK: &[u8; 5] = b"+OK\r\n";
    const RESP_NULL: &[u8; 5] = b"$-1\r\n";

    let mut new_db = RedisDatabase::new();
    //if file exists use the first db in the indexing, else use a new db
    if db_filename.is_some() && dir.is_some() {
        let file = db_filename.as_ref().unwrap();

        let directory = dir.as_ref().unwrap();
        //eprintln!("OUND DIR");
        // create a new file path
        let path = Path::new(directory).join(file);
        match read_rdb_file(path) {
            Ok(rdb) => {
                //eprintln!("RDB ILE {:?}", rdb);
                let opt_db = rdb.databases.get(&0u8);
                if let Some(storage_db) = opt_db {
                    //eprintln!("storage db:{:?}", storage_db);
                    new_db = storage_db.clone();
                }
                //eprintln!("USING STORAGE DB: {:?}", new_db);
            }
            Err(_e) => {
                //eprintln!("failed to read from rdb file {:?}, USING NEWDB", e);
            }
        }
    }
    loop {
        let Some(all_lines) = decode_bulk_string(&stream) else {
            break;
        };
        //eprintln!("ALL LINES:{:?}", all_lines);

        let cmd = &all_lines[1];

        match cmd.to_lowercase().as_str() {
            "ping" => {
                stream.write_all(b"+PONG\r\n").unwrap();
            }
            "echo" => {
                let resp = [b"+", all_lines[3].as_bytes(), b"\r\n"].concat();
                stream.write_all(&resp).unwrap();
            }
            /*
             * SET SECTION
             * */
            "set" => {
                let k = all_lines[3].clone();
                let v = all_lines[5].clone();

                if all_lines.len() > 6 {
                    new_db.insert(
                        k,
                        RedisValue {
                            value: v,
                            expires_at: None,
                        },
                    );
                    match all_lines[7].to_lowercase().as_str() {
                        "px" => {
                            let time_arg: u64 = all_lines[9].parse()?;
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64; //eprintln!("got MILLISECONDS expiry:{time_arg}");
                            let end_time_s = now + time_arg;
                            //eprintln!("AT: {now}, MSexpiry:{time_arg},end:{end_time_s}");
                            let use_expiry = Some(Expiration::Milliseconds(end_time_s));
                            let curr_val = new_db.data.get_mut(&all_lines[3]).unwrap();
                            curr_val.expires_at = use_expiry;
                        }
                        "ex" => {
                            let time_arg: u32 = all_lines[9].parse()?;
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs();
                            let end_time_s = now as u32 + time_arg;

                            //eprintln!("AT: {now}, got SECONDS expiry:{time_arg}, expected end:{end_time_s}");
                            let use_expiry = Some(Expiration::Seconds(end_time_s as u32));
                            let curr_val = new_db.data.get_mut(&all_lines[3]).unwrap();
                            curr_val.expires_at = use_expiry;
                        }
                        _ => {
                            return Err(Box::new(RdbError::UnsupportedFeature(
                                "WRONG SET ARGUMENTS",
                            )))
                        }
                    }
                    //eprintln!("before inserting in db, expiry:{:?}", use_expiry);
                } else {
                    new_db.insert(
                        k,
                        RedisValue {
                            value: v,
                            expires_at: None,
                        },
                    );
                }
                stream.write_all(RESP_OK).unwrap();
            }

            /*
             * GET SECTION
             * */
            "get" => {
                //eprintln!("IN GET");
                let get_key = &all_lines[3];

                if let Some(res) = new_db.get(&get_key) {
                    if res.expires_at.is_none()
                        || (res.expires_at.is_some()
                            && !res.expires_at.as_ref().unwrap().is_expired())
                    {
                        //eprintln!("in get TIME STILL");
                        let resp = get_bulk_string(&res.value);
                        stream.write_all(&resp).unwrap();
                    } else {
                        //eprintln!("db: {:?}", new_db);
                        // eprintln!(
                        //     "expired: {:?}",
                        //     res.expires_at.as_ref().unwrap().is_expired()
                        // );
                        //eprintln!("in get TIME OVER, removing expired key, {}", get_key);
                        new_db.data.remove(get_key);
                        stream.write_all(RESP_NULL).unwrap();
                    }
                } else {
                    //eprintln!("IN GET OUND NOTHING");
                    stream.write_all(RESP_NULL).unwrap();
                }
            }

            /*
             *CONIG
             * */
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
                let path: PathBuf;
                if db_filename.is_some() && dir.is_some() {
                    //eprintln!("OUND FILE");
                    let file = db_filename.as_ref().unwrap();

                    let directory = dir.as_ref().unwrap();
                    //eprintln!("OUND DIR");
                    // create a new file path
                    // then write current hashmap to rdb
                    path = Path::new(directory).join(file);
                } else {
                    path = env::current_dir().unwrap().join("dump.rdb");
                }

                //eprintln!("USING PATH:{:?}", &path);

                let mut file = File::open(&path)?;
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer)?;

                //eprintln!("Printin rdb as HEX");
                print_hex::print_hex_dump(&buffer);
                match read_rdb_file(path) {
                    Ok(rdb) => {
                        let ret_keys = read_rdb_keys(rdb, all_lines[3].clone());

                        //EXAMPLE: *1\r\n$3\r\nfoo\r\n
                        let _ = stream.write_all(
                            &[b"*", ret_keys.len().to_string().as_bytes(), b"\r\n"].concat(),
                        );
                        ret_keys.iter().enumerate().for_each(|(_, e)| {
                            let _ = stream.write_all(&get_bulk_string(e));
                        });
                    }
                    Err(_e) => {
                        //eprintln!("failed to read from rdb file {:?}", e);
                        stream.write_all(RESP_NULL).unwrap();
                    }
                }
            }
            "save" => {
                //eprintln!("IN SAVE");
                let mut path: PathBuf;
                if db_filename.is_some() && dir.is_some() {
                    // create a new file path then write current hashmap to rdb
                    path = Path::new(dir.as_ref().unwrap()).join(db_filename.as_ref().unwrap());

                    //eprintln!("Path:{:?}", &path);
                    let mut new_rdb = RdbFile {
                        version: "0011".to_string(),
                        metadata: HashMap::new(),
                        databases: HashMap::new(),
                    };

                    new_rdb
                        .metadata
                        .insert("redis-version".to_string(), "6.0.16".to_string());
                    //eprintln!("IN Save, using map {:?}", new_db);
                    new_rdb.databases.insert(0, new_db.clone());
                    //eprintln!("Creating a new rdb with {:?}", new_rdb);

                    let _ = write_rdb_file(path, &new_rdb);

                    //eprintln!("after SAVE writing to file");
                    stream.write_all(RESP_OK)?;
                } else {
                    //eprintln!("Creating DUMMY in curr dir");
                    path = env::current_dir().unwrap();
                    path.push("dump.rdb");
                    print_hex::create_dummy_rdb(&path)?;
                    stream.write_all(RESP_OK)?;
                    // no need for data as it already mocked
                }
            }
            "info" => {
                //if there is an extra key arg
                if all_lines.len() > 5 {
                    let info_key = &all_lines[5];
                    let mut use_resp = String::new();
                    match info_key.to_lowercase().as_str() {
                        ROLE => {
                            use_resp.push_str("role:");
                            use_resp.push_str(info_fields.get(ROLE).unwrap());
                        }
                        "replication" => {
                            let fields = [ROLE, MASTER_REPL_ID, MASTER_REPL_OFFSET];
                            fields.iter().for_each(|elem| {
                                use_resp.push_str(elem);
                                use_resp.push_str(info_fields.get(elem).unwrap());
                            });
                        }
                        _ => {}
                    }
                    stream.write_all(&get_bulk_string(&use_resp))?;
                } else {
                    for (k, v) in info_fields.iter() {
                        let mut use_val = k.to_string();
                        use_val.push_str(":");
                        use_val.push_str(v);
                        stream.write_all(&get_bulk_string(&use_val))?;
                    }
                }
            }
            _unrecognized_cmd => {
                return Err(Box::new(RdbError::UnsupportedFeature(
                    "UNRECOGNIZED COMMAND",
                )))
            }
        }
    }
    Ok(())
}

fn get_bulk_string(res: &String) -> Vec<u8> {
    let res_size = res.len();
    [
        b"$",
        res_size.to_string().as_bytes(),
        b"\r\n",
        res.as_bytes(),
        b"\r\n",
    ]
    .concat()
}

fn read_rdb_keys(rdb: RdbFile, search_key: String) -> Vec<String> {
    //eprintln!("Successful rdb read");
    let mut ret_keys = Vec::new();
    //get by index
    // TODO! instead of hardcoding, find the latest key, i.e largest num
    if let Some(db) = rdb.databases.get(&0) {
        //eprintln!("GOT DB ROM RDB FILE {:?}", db);
        match search_key.as_str() {
            "*" => {
                //eprintln!("GOT * search");
                db.data.clone().into_iter().for_each(|(k, _)| {
                    ret_keys.push(k);
                });
            }
            _others => {
                let search_strings: Vec<&str> = search_key.split("*").collect();

                //eprintln!(
                //      "GOT OTHERS search:{others}, searching with {:?}",
                //      search_strings
                //  );
                db.data.clone().into_iter().for_each(|(k, _)| {
                    if search_strings.iter().all(|e| k.contains(e)) {
                        ret_keys.push(k);
                    }
                });
            }
        }
    }
    //eprintln!("All KEYS to return:{:?}", ret_keys);
    ret_keys
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
     * if next returns None then no more lines
     */
    let arr_length = my_iter.next()?;

    /*
    * for each element we'll have 2 lines, one with the size and the other with the text
        so arr_length will ne provided num of elements * 2
    */
    let arr_length = arr_length.expect("failed to unwrap arr length line from buf")[1..]
        .parse::<usize>()
        .expect("failed to get bulk string element num from stream")
        * 2;
    for _ in 0..arr_length {
        all_lines.push(my_iter.next()?.unwrap());
    }
    Some(all_lines)
}
