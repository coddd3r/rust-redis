#![allow(unused_imports)]
use std::char::decode_utf16;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{self, File};
use std::io::{prelude::*, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{env, usize};

use codecrafters_redis::print_hex::create_dummy_rdb;
use codecrafters_redis::{
    print_hex, read_rdb_file, write_rdb_file, Expiration, RdbError, RdbFile, RedisDatabase,
    RedisValue,
};

mod threadpool;
use threadpool::ThreadPool;

use crate::utils::get_bulk_string;
mod utils;

const ROLE: &str = "role";
const MASTER: &str = "master";
const SLAVE: &str = "slave";
const MASTER_REPL_OFFSET: &str = "master_repl_offset";
const MASTER_REPL_ID: &str = "master_replid";
const REPL_CONF: &str = "REPLCONF";
const LISTENING_PORT: &str = "listening-port";
const PSYNC: &str = "PSYNC";
const FULLRESYNC: &str = "FULLRESYNC";

const RESP_OK: &[u8; 5] = b"+OK\r\n";
const RESP_NULL: &[u8; 5] = b"$-1\r\n";

fn main() {
    let id = utils::random_id_gen();
    let mut info_fields: HashMap<&str, String> = HashMap::new();
    info_fields.insert(ROLE, MASTER.to_string());
    eprintln!("ID:{:?}", id);
    //info_fields.insert("id", id);

    let arg_list = std::env::args();
    //eprintln!("ARGS:{:?}", &arg_list);
    let mut dir = None;
    let mut db_filename = None;
    let mut b = arg_list.into_iter();
    let mut full_port = String::from("127.0.0.1:");
    let mut port_found = false;
    let mut short_port = String::new();
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
            "--port" => {
                port_found = true;
                if let Some(p) = b.next() {
                    full_port.push_str(&p);
                    short_port = p;
                } else {
                    full_port.push_str("6379");
                }
            }
            "--replicaof" => {
                let curr_role = info_fields.get_mut(ROLE).unwrap();
                *curr_role = SLAVE.to_string();

                fn read_response(st: &TcpStream, n: usize) -> String {
                    let mut buf_reader = BufReader::new(st.try_clone().unwrap());
                    let mut use_buf = String::new();
                    let _ = buf_reader.read_line(&mut use_buf);

                    eprintln!("{n}th handshake done, response:{}", use_buf);
                    use_buf
                }
                if let Some(master) = b.next() {
                    eprintln!("is replica of:{master}");
                    let master_port: Vec<_> = master.split_whitespace().collect();
                    let send_to: String =
                        [master_port[0], ":", master_port[1]].into_iter().collect();
                    eprintln!("connecting to master on {send_to}");
                    match TcpStream::connect(send_to) {
                        Ok(mut conn) => {
                            let buf = b"*1\r\n$4\r\nPING\r\n";
                            conn.write_all(buf).expect("FAILED TO PING master");
                            //confirm listening on to main node
                            let _ = read_response(&conn, 1);
                            let repl_port =
                                utils::get_repl_bytes(REPL_CONF, LISTENING_PORT, &short_port);

                            conn.write_all(&repl_port)
                                .expect("FAILED TO REPLCONF master");
                            let _ = read_response(&conn, 2);

                            let repl_capa = utils::get_repl_bytes(REPL_CONF, "capa", "psync2");

                            conn.write_all(&repl_capa)
                                .expect("FAILED TO REPLCONF master");
                            let master_response = read_response(&conn, 3);

                            if master_response == String::from_utf8_lossy(RESP_OK) {
                                let pysnc_resp = utils::get_repl_bytes(PSYNC, "?", "-1");
                                conn.write_all(&pysnc_resp).expect("Failed to send PSYNC");

                                let _ = read_response(&conn, 4);
                                // create dummy rdb for response
                                let dummy_rdb_path =
                                    env::current_dir().unwrap().join("dump-dummy.rdb");
                                create_dummy_rdb(&dummy_rdb_path.as_path())
                                    .expect("FAILED TO MAKE DUMMY RDB");

                                if let Ok(response_rdb_bytes) = fs::read(dummy_rdb_path) {
                                    eprintln!("writing rdb len {}", response_rdb_bytes.len());
                                    conn.write_all(
                                        &[
                                            response_rdb_bytes.len().to_string().as_bytes(),
                                            b"\r\n",
                                            &response_rdb_bytes,
                                        ]
                                        .concat(),
                                    )
                                    .expect("FAILED TO WRITE SIZE LINE");

                                    let _ = read_response(&conn, 5);
                                };
                            } else {
                                eprintln!("GOT UNEXPECTED RESPONSE TO PSYNC")
                            }
                        }
                        Err(e) => eprintln!("FAILED CONNECTION to master{:?}", e),
                    }
                }
            }
            _ => {}
        }
    }

    let rl = info_fields.get(ROLE);
    if let Some(r) = rl {
        if r == MASTER {
            info_fields.insert(MASTER_REPL_ID, id.clone());
            info_fields.insert(MASTER_REPL_OFFSET, "0".to_string());
        } else {
            info_fields.insert("repl_id", id.clone());
        }
    }

    if !port_found {
        full_port.push_str("6379");
    }

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
    let mut new_db = RedisDatabase::new();
    //if file exists use the first db in the indexing, else use a new db
    if db_filename.is_some() && dir.is_some() {
        let file = db_filename.as_ref().unwrap();
        let directory = dir.as_ref().unwrap();
        let path = Path::new(directory).join(file);

        match read_rdb_file(path) {
            Ok(rdb) => {
                let opt_db = rdb.databases.get(&0u8);
                if let Some(storage_db) = opt_db {
                    new_db = storage_db.clone();
                }
            }
            Err(_e) => {}
        }
    }

    loop {
        let Some(all_lines) = utils::decode_bulk_string(&stream) else {
            break;
        };
        if all_lines.len() <= 1 {
            eprintln!("COMMAND TOO SHORT: LINES {:?}", all_lines);
            stream.write_all(RESP_OK).unwrap();
            continue;
        }
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
                stream.write_all(RESP_OK)?;
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
                        let resp = utils::get_bulk_string(&res.value);
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
             *CONFIG
             * */
            "config" => {
                let config_command = all_lines[3].to_lowercase();
                let config_field = all_lines[5].to_lowercase();

                fn config_response(field_type: &str, field_name: &str) -> Vec<u8> {
                    let mut resp: Vec<u8> = Vec::new();
                    resp.extend_from_slice(b"*2\r\n");
                    resp.extend(get_bulk_string(field_type));
                    resp.extend(get_bulk_string(field_name));
                    resp
                }
                match config_command.as_str() {
                    "get" => match config_field.as_str() {
                        "dir" => {
                            if let Some(dir_name) = &dir {
                                let resp = config_response(&config_field, dir_name);
                                stream.write_all(&resp).unwrap();
                            } else {
                                stream.write_all(RESP_NULL)?;
                            }
                        }
                        "dbfilename" => {
                            if let Some(db_name) = &db_filename {
                                let resp = config_response(&config_field, db_name);
                                stream.write_all(&resp).unwrap();
                            } else {
                                stream.write_all(RESP_NULL)?;
                            }
                        }
                        _ => {
                            eprintln!("UNRECOGNIZED GET CONFIG FIELD");
                        }
                    },
                    _ => {
                        eprintln!("UNRECOGNIZED CONFIG COMMAND")
                    }
                }
            }

            "keys" => {
                let path: PathBuf;
                if db_filename.is_some() && dir.is_some() {
                    //eprintln!("OUND FILE");
                    let file = db_filename.as_ref().unwrap();

                    let directory = dir.as_ref().unwrap();
                    //eprintln!("OUND DIR");
                    // write current hashmap to rdb
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
                        let ret_keys = utils::read_rdb_keys(rdb, all_lines[3].clone());

                        //EXAMPLE: *1\r\n$3\r\nfoo\r\n
                        let _ = stream.write_all(
                            &[b"*", ret_keys.len().to_string().as_bytes(), b"\r\n"].concat(),
                        );
                        ret_keys.iter().enumerate().for_each(|(_, e)| {
                            let _ = stream.write_all(&utils::get_bulk_string(e));
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
                eprintln!("IN INFO SECTION");
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
                    eprintln!("INFO RESPONSE:{:?}", use_resp);
                    stream.write_all(&utils::get_bulk_string(&use_resp))?;
                } else {
                    eprintln!("IN INFO ELSE");
                    let mut use_val = String::new();

                    info_fields
                        .iter()
                        .for_each(|(k, v)| use_val.extend([k, ":", v, "\r\n"]));
                    // remove the last CRLF
                    let info_res = utils::get_bulk_string(&use_val[..use_val.len() - 2]);
                    eprintln!("RESPONSE:{:?}", String::from_utf8_lossy(&info_res));
                    stream.write_all(&info_res)?;
                }
                eprintln!("AFTER INFO SECTION");
            }
            "replconf" => {
                stream.write_all(RESP_OK)?;
                eprintln!("WROTE ok to replconf");
            }
            "psync" => {
                let resync_response = [
                    b"+",
                    FULLRESYNC.as_bytes(),
                    b" ",
                    info_fields.get(MASTER_REPL_ID).unwrap().as_bytes(),
                    b" ",
                    info_fields.get(MASTER_REPL_OFFSET).unwrap().as_bytes(),
                    b"\r\n",
                ]
                .concat();
                stream.write_all(&resync_response)?;
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
