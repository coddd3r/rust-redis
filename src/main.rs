#![allow(unused_imports)]
use std::char::decode_utf16;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{self, File};
use std::io::{prelude::*, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{env, usize};

use codecrafters_redis::print_hex::{create_dummy_rdb, print_hex_dump};
use codecrafters_redis::{
    print_hex, read_rdb_file, write_rdb_file, Expiration, RdbError, RdbFile, RedisDatabase,
    RedisValue,
};

mod threadpool;
use threadpool::ThreadPool;
use tokio::sync::broadcast;

use crate::utils::{
    broadcast_commands, decode_bulk_string, get_bulk_string, get_port, handle_get, handle_set,
    read_response, write_resp_arr,
};
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
const DEFAULT_PORT: &str = "6379";

const RESP_OK: &[u8; 5] = b"+OK\r\n";
const RESP_NULL: &[u8; 5] = b"$-1\r\n";

//#[derive(Debug, Default)]
//struct BroadCastInfo {
//    ports: Vec<String>,
//    //connection: Option<TcpStream>,
//    connections: Vec<Arc<Mutex<TcpStream>>>,
//}
////#[derive(Debug, Default, Clone)]
//enum ConnectionType {
//    #[default]
//    RegularClient,
//    ReplicationMaster,
//}

#[derive(Debug, Clone)]
struct ConnectionInfo {
    //stream_id: usize, // Unique identifier for each connection
    stream: Arc<Mutex<TcpStream>>,
}

#[derive(Debug)]
struct BroadCastInfo {
    connections: Vec<ConnectionInfo>,
    next_id: usize, // Counter for generating unique IDs
    ports: Vec<String>,
}

impl BroadCastInfo {
    fn new() -> Self {
        BroadCastInfo {
            connections: Vec::new(),
            next_id: 0,
            ports: Vec::new(),
        }
    }

    fn add_connection(&mut self, stream: TcpStream) -> usize {
        let id = self.next_id;
        self.connections.push(ConnectionInfo {
            //     stream_id: id,
            stream: Arc::new(Mutex::new(stream)),
        });
        self.next_id += 1;
        id
    }
}

fn main() {
    let id = utils::random_id_gen();
    let mut info_fields: HashMap<String, String> = HashMap::new();
    info_fields.insert(String::from(ROLE), MASTER.to_string());
    eprintln!("ID:{:?}", id);
    //info_fields.insert("id", id);

    let arg_list = std::env::args();
    //eprintln!("ARGS:{:?}", &arg_list);
    let mut dir = None;
    let mut db_filename = None;
    let mut full_port = String::from("127.0.0.1:");
    let mut port_found = false;
    let mut short_port = String::new();

    let stream_pool = ThreadPool::new(4);
    let mut master_port: Option<String> = None;
    //let mut master_conn: Option<TcpStream> = None;
    let broadcast_info = BroadCastInfo::new();
    let broadcast_info: Arc<Mutex<BroadCastInfo>> = Arc::new(Mutex::new(broadcast_info));
    let new_db = RedisDatabase::new();

    let mut new_db = Arc::new(Mutex::new(new_db));

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
                if db_filename.is_some() && dir.is_some() {
                    let file = db_filename.as_ref().unwrap();
                    let directory = dir.as_ref().unwrap();
                    let path = Path::new(directory).join(file);

                    match read_rdb_file(path) {
                        Ok(rdb) => {
                            let opt_db = rdb.databases.get(&0u8);
                            if let Some(storage_db) = opt_db {
                                new_db = Arc::new(Mutex::new(storage_db.clone()));
                            }
                        }
                        Err(_e) => {}
                    }
                }
            }
            "--port" => {
                port_found = true;
                if let Some(p) = b.next() {
                    full_port.push_str(&p);
                    short_port = p;
                } else {
                    full_port.push_str(DEFAULT_PORT);
                }
            }
            "--replicaof" => {
                let curr_role = info_fields.get_mut(ROLE).unwrap();
                *curr_role = SLAVE.to_string();

                if let Some(master) = b.next() {
                    eprintln!("Running at:{full_port} is replica of:{master}");

                    /////////
                    let master_p: Vec<_> = master.split_whitespace().collect();
                    let send_to: String = [master_p[0], ":", master_p[1]].into_iter().collect();
                    master_port = Some(send_to.clone());
                    eprintln!("connecting to master on {send_to}");
                    match TcpStream::connect(send_to) {
                        Ok(conn) => {
                            {
                                let mut b_lock = broadcast_info.lock().unwrap();
                                b_lock.add_connection(conn.try_clone().unwrap());
                            }
                            {
                                let i_fields = info_fields.clone();
                                let m_port = master_port.clone();
                                let use_db = Arc::clone(&new_db);
                                let b_info = Arc::clone(&broadcast_info);
                                let short_port = short_port.clone();

                                let use_conn = conn
                                    .try_clone()
                                    .expect("failed to clone connection to master");
                                stream_pool.execute(move || {
                                    let res = handle_master(
                                        use_conn.try_clone().expect("failed to clone master conn"),
                                        i_fields,
                                        b_info,
                                        &m_port,
                                        &short_port,
                                        &use_db,
                                    );
                                    match res {
                                        Ok(_) => {}
                                        Err(e) => {
                                            eprintln!("Error handling Master {}", e);
                                        }
                                    }
                                });
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
            info_fields.insert(String::from(MASTER_REPL_ID), id.clone());
            info_fields.insert(String::from(MASTER_REPL_OFFSET), "0".to_string());
        } else {
            info_fields.insert(String::from("repl_id"), id.clone());
        }
    }

    if !port_found {
        full_port.push_str(DEFAULT_PORT);
    }

    let listener = TcpListener::bind(&full_port).unwrap();
    eprintln!("Listening on port:{full_port}");

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");
                let dir_arg = dir.clone();
                let db_arg = db_filename.clone();
                let i_fields = info_fields.clone();
                let m_port = master_port.clone();

                let b_info = Arc::clone(&broadcast_info);
                let use_db = Arc::clone(&new_db);

                stream_pool.execute(move || {
                    let res =
                        handle_client(_stream, dir_arg, db_arg, i_fields, b_info, &m_port, &use_db);
                    match res {
                        Ok(_) => {}
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

fn handle_master(
    mut stream: TcpStream,
    info_fields: HashMap<String, String>,
    broadcast_info: Arc<std::sync::Mutex<BroadCastInfo>>,
    master_port: &Option<String>,
    replica_port: &str,
    new_db: &Arc<Mutex<RedisDatabase>>,
) -> Result<(), Box<dyn Error>> {
    eprintln!("\n\nHANDLING MASTER\n");

    eprintln!("CLIENT HANDLING MASTER , master port:{:?}", master_port);
    eprintln!("CLIENT HANDLING MASTER , info:{:?}", broadcast_info);

    {
        eprintln!("HANDLING HANDSHAKE");
        let buf = write_resp_arr(vec!["PING"]);
        stream.write_all(&buf).expect("FAILED TO PING master");
        let _ = read_response(&stream, Some(1));

        let repl_port = utils::get_repl_bytes(REPL_CONF, LISTENING_PORT, &replica_port);
        stream
            .write_all(&repl_port)
            .expect("FAILED to reply master");
        let _ = read_response(&stream, Some(2));

        let repl_capa = utils::get_repl_bytes(REPL_CONF, "capa", "psync2");
        stream
            .write_all(&repl_capa)
            .expect("FAILED TO reply master");
        let _ = read_response(&stream, Some(3));

        let pysnc_resp = utils::get_repl_bytes(PSYNC, "?", "-1");
        stream.write_all(&pysnc_resp).expect("Failed to send PSYNC");
        let _ = read_response(&stream, Some(4));

        let mut first_line = String::new();
        let mut bulk_reader = BufReader::new(stream.try_clone().unwrap());
        bulk_reader.read_line(&mut first_line).unwrap();
        //let mut rdb_bytes = Vec::new();

        if first_line.is_empty() {
            eprintln!("EMPTY FIRST LINE IN RDB RECEIVED");
        } else {
            eprintln!("IN HANDSHAKE READING FROM STREAM WITH SIZE:{first_line}");
            let rdb_len = first_line[1..]
                .trim()
                .parse::<usize>()
                .expect("failed to parse rdb length");

            //let rdb_bytes = read_db_from_stream(&first_line[1..], bulk_reader);
            eprintln!("IGNORING RDB BYTES");
            bulk_reader.consume(rdb_len);
            //eprintln!("RDB IN HANDSHAKE:{:?}", rdb_bytes);
            //decode_rdb(rdb_bytes);
        }
    }

    loop {
        let Some(all_lines) = utils::decode_bulk_string(&stream) else {
            break;
        };
        if all_lines.len() <= 1 {
            eprintln!("COMMAND TOO SHORT: LINES {:?}", all_lines);
            //stream.write_all(RESP_OK).unwrap();
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

            "set" => {
                eprintln!("IN handle master SET");

                if all_lines.len() < 6 {
                    stream.write_all(RESP_NULL)?;
                    continue;
                }
                if info_fields.get(ROLE).unwrap() == MASTER {
                    broadcast_commands(all_lines.as_slice(), &broadcast_info);
                }
                let k = all_lines[3].clone();
                let v = all_lines[5].clone();

                if all_lines.len() > 6 {
                    handle_set(k, v, &new_db, Some((&all_lines[7], &all_lines[9])))?;
                } else {
                    handle_set(k, v, &new_db, None)?;
                }
            }

            "command" => {
                eprintln!("INITIATION, no command");
            }

            _ => unreachable!(),
        }
    }

    Ok(())
}

fn handle_client(
    mut stream: TcpStream,
    dir: Option<String>,
    db_filename: Option<String>,
    info_fields: HashMap<String, String>,
    broadcast_info: Arc<std::sync::Mutex<BroadCastInfo>>,
    master_port: &Option<String>,
    new_db: &Arc<Mutex<RedisDatabase>>,
) -> Result<(), Box<dyn Error>> {
    let sent_by_main = master_port.is_some() && get_port(&stream) == *master_port;
    if sent_by_main {
        eprintln!("\n\n\nSENT BY MAIN\n\n");
    };

    loop {
        //eprintln!("HANDLING CLIENT LOOP info:{:?}", &broadcast_info);
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

            "set" => {
                eprintln!("IN handle client SET");

                if all_lines.len() < 6 {
                    stream.write_all(RESP_NULL)?;
                    continue;
                }
                {
                    if info_fields.get(ROLE).unwrap() == MASTER {
                        broadcast_commands(all_lines.as_slice(), &broadcast_info);
                    }
                    let k = all_lines[3].clone();
                    let v = all_lines[5].clone();

                    if all_lines.len() > 6 {
                        let r = handle_set(k, v, new_db, Some((&all_lines[7], &all_lines[9])));
                        if r.is_ok() {
                            stream.write_all(RESP_OK)?;
                        }
                    } else {
                        let r = handle_set(k, v, new_db, None);
                        if r.is_ok() {
                            stream.write_all(RESP_OK)?;
                        }
                    }
                    eprintln!("MASTER FINISHED SET");
                }
            }

            /*
             * GET SECTION
             * */
            "get" => {
                if all_lines.len() <= 3 {
                    stream.write_all(RESP_NULL)?;
                    continue;
                }
                eprintln!("IN handle client GET");
                let get_key = &all_lines[3];
                handle_get(get_key, &mut stream, new_db)?;
            }

            /*
             *CONFIG
             * */
            "config" => {
                let config_command = all_lines[3].to_lowercase();
                let config_field = all_lines[5].to_lowercase();

                fn config_response(field_type: &str, field_name: &str) -> Vec<u8> {
                    write_resp_arr(vec![field_type, field_name])
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

            //KEYS
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

            //SAVE
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
                    {
                        let lk = new_db.lock().expect("failed to lock db in save");
                        new_rdb.databases.insert(0, lk.clone().try_into()?);
                        //eprintln!("Creating a new rdb with {:?}", new_rdb);
                    }
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

            //INFO
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
                                use_resp.push_str(info_fields.get(*elem).unwrap());
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

            //REPL
            "replconf" => {
                eprintln!("in repl conf alllines:{:?}", all_lines);
                let is_listener = all_lines.iter().any(|e| e == LISTENING_PORT);
                if is_listener {
                    let mut lk = broadcast_info.lock().unwrap();
                    // lk.ports.push(String::from("1-standin"));
                    lk.ports.push(all_lines[5].clone());
                }
                eprintln!("is LISTENER handshake?:{is_listener}");
                eprintln!("after repl pushing ports:{:?}", broadcast_info);
                stream.write_all(RESP_OK)?;
                eprintln!("WROTE ok to replconf");
            }

            //PSYNC
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

                {
                    let mut lk = broadcast_info.lock().unwrap();
                    lk.connections.push(ConnectionInfo {
                        stream: Arc::new(Mutex::new(stream.try_clone().unwrap())),
                    });
                }
                {
                    let mut lk = broadcast_info.lock().unwrap();
                    let mut s = lk.connections.last_mut().unwrap().stream.lock().unwrap();
                    //stream.write_all(&resync_response)?;
                    eprint!("\n\n\nSENDING RESYNC RESPONSE USING SAVED STREAM\n\n");
                    s.write_all(&resync_response)?;

                    eprint!("\n\n\nSENDING RDB USING SAVED STREAM:{:?}\n\n", s);
                    //let dummy_rdb_path = env::current_dir().unwrap().join("empty.rdb");
                    let dummy_rdb_path = env::current_dir().unwrap().join("dump_dummy.rdb");
                    create_dummy_rdb(&dummy_rdb_path.as_path()).expect("FAILED TO MAKE DUMMY RDB");
                    if let Ok(response_rdb_bytes) = fs::read(dummy_rdb_path) {
                        eprintln!("IN MASTER SENDING RB");
                        //stream
                        eprintln!("writing rdb len {}", response_rdb_bytes.len());
                        print_hex_dump(&response_rdb_bytes);

                        s.write_all(
                            &[
                                b"$",
                                response_rdb_bytes.len().to_string().as_bytes(),
                                b"\r\n",
                            ]
                            .concat(),
                        )
                        .expect("FAILED TO WRITE SIZE LINE");

                        // stream
                        //     .write_all(&response_rdb_bytes)
                        //     .expect("failed to write rdb bytes in response to hadnshake");
                        s.write_all(&response_rdb_bytes)
                            .expect("failed to write rdb bytes in response to hadnshake");
                    };
                }
            }

            "command" => {
                eprintln!("INITIATION, no command");
                return Ok(());
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
