#![allow(unused_imports)]
use std::char::decode_utf16;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{self, File};
use std::io::{prelude::*, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::{self, sleep};
use std::time::Instant;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{env, usize};

use codecrafters_redis::print_hex::{create_dummy_rdb, print_hex_dump};
use codecrafters_redis::{
    print_hex, read_rdb_file, write_rdb_file, Expiration, RdbError, RdbFile, RedisDatabase,
    RedisValue,
};

//mod client_handler;
mod resp_parser;
mod threadpool;
mod utils;

use threadpool::ThreadPool;
use tokio::sync::broadcast;

//use crate::utils::{config_response, decode_bulk_string, get_bulk_string, handle_get, handle_set};
use crate::utils::{get_port, handle_set};

use crate::resp_parser::{BroadCastInfo, RespConnection};

const ROLE: &str = "role";
const MASTER: &str = "master";
const SLAVE: &str = "slave";
const MASTER_REPL_OFFSET: &str = "master_repl_offset";
const MASTER_REPL_ID: &str = "master_replid";
const REPL_CONF: &str = "REPLCONF";
const GETACK: &str = "GETACK";
const DIFF: &str = "DIFF";
const ACK: &str = "ACK";
const LISTENING_PORT: &str = "listening-port";
const PSYNC: &str = "PSYNC";
const FULLRESYNC: &str = "FULLRESYNC";
const DEFAULT_PORT: &str = "6379";

const RESP_OK: &[u8; 5] = b"+OK\r\n";
const RESP_NULL: &[u8; 5] = b"$-1\r\n";

fn main() {
    let id = utils::random_id_gen();
    let mut info_fields: HashMap<String, String> = HashMap::new();
    info_fields.insert(String::from(ROLE), MASTER.to_string());
    eprintln!("ID:{:?}", id);
    //info_fields.insert("id", id);

    let arg_list = std::env::args();
    eprintln!("ARGS:{:?}", &arg_list);
    let mut dir = None;
    let mut db_filename = None;
    let mut full_port = String::from("127.0.0.1:");
    let mut port_found = false;
    let mut short_port = String::new();

    let stream_pool = ThreadPool::new(15);
    let mut master_port: Option<String> = None;
    //let mut master_conn: Option<TcpStream> = None;
    let broadcast_info: Arc<Mutex<BroadCastInfo>> = Arc::new(Mutex::new(BroadCastInfo::new()));
    let new_db = RedisDatabase::new();

    let mut new_db = Arc::new(Mutex::new(new_db));

    let mut b = arg_list.into_iter();
    while let Some(a) = b.next() {
        match a.as_str() {
            "--dir" => {
                dir = b.next();
                eprintln!("GOT DIR");
            }
            "--dbfilename" => {
                db_filename = b.next();
                eprintln!("GOT ILE");
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
                    master_port = Some(master_p[1].to_string());
                    eprintln!("connecting to master on {send_to}");
                    match TcpStream::connect(send_to) {
                        Ok(conn) => {
                            //let s = Arc::new(Mutex::new(conn));
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
                                //let use_stream = Arc::clone(&s);
                                let use_stream = conn.try_clone().unwrap();

                                stream_pool.execute(move || {
                                    let res = handle_client(
                                        use_stream,
                                        None,
                                        None,
                                        i_fields,
                                        b_info,
                                        &Some(short_port.as_str()),
                                        &m_port,
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
                        Err(e) => {
                            eprintln!("FAILED CONNECTION to master{:?}", e);
                        }
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
                //println!("accepted new connection");
                let dir_arg = dir.clone();
                let db_arg = db_filename.clone();
                let i_fields = info_fields.clone();
                let m_port = master_port.clone();

                let b_info = Arc::clone(&broadcast_info);
                let use_db = Arc::clone(&new_db);
                //let s = Arc::new(Mutex::new(_stream));
                let s = _stream.try_clone().unwrap();

                let short_port = short_port.clone();

                stream_pool.execute(move || {
                    let res = handle_client(
                        s,
                        dir_arg,
                        db_arg,
                        i_fields,
                        b_info,
                        &Some(short_port.as_str()),
                        &m_port,
                        &use_db,
                    );
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

fn handle_client(
    //stream: Arc<Mutex<TcpStream>>,
    stream: TcpStream,
    dir: Option<String>,
    db_filename: Option<String>,
    info_fields: HashMap<String, String>,
    broadcast_info: Arc<Mutex<BroadCastInfo>>,
    replica_port: &Option<&str>,
    master_port: &Option<String>,
    new_db: &Arc<Mutex<RedisDatabase>>,
) -> Result<(), Box<dyn Error>> {
    eprintln!(
        "handling_connection, master_port:{:?}, stream port:{:?}",
        master_port,
        get_port(&stream)
    );

    let sent_by_main = master_port.is_some() && get_port(&stream) == *master_port;
    eprintln!(
        "IS MASTER?{:?}, master_port:{:?}",
        master_port.is_some(),
        master_port
    );

    //let mut conn = RespConnection::new(Arc::clone(&stream));
    let mut conn = RespConnection::new(stream.try_clone().unwrap());

    if sent_by_main {
        conn.is_master = true;
        eprintln!("\n\n\n\nHANDLING HANDSHAKE\n\n\n\n\n");
        conn.write_to_stream(&conn.format_resp_array(&["PING"]));
        let res = conn.try_read_command();
        sleep(Duration::from_millis(10));
        eprintln!("Read result: {:?}", res);

        let use_bytes = conn.format_resp_array(&[REPL_CONF, LISTENING_PORT, replica_port.unwrap()]);
        conn.write_to_stream(&use_bytes);
        let res = conn.try_read_command();
        sleep(Duration::from_millis(10));
        eprintln!("Read result: {:?}", res);

        conn.write_to_stream(&conn.format_resp_array(&[REPL_CONF, "capa", "psync2"]));
        let res = conn.try_read_command();
        sleep(Duration::from_millis(10));
        eprintln!("Read result: {:?}", res);

        conn.write_to_stream(&conn.format_resp_array(&[PSYNC, "?", "-1"]));
        //ignore the last sent after psync
    }

    let mut write_command: Vec<_> = Vec::new();

    loop {
        match conn.try_read_command() {
            Ok(Some(commands)) => {
                eprintln!("ALL COMMANDS:{:?}", commands);
                eprintln!("current broadcast info:{:?}", broadcast_info);
                for all_lines in commands {
                    if all_lines.len() < 1 {
                        eprintln!("COMMAND TOO SHORT: LINES {:?}", all_lines);
                        continue;
                    }
                    eprintln!("ALL LINES:{:?}", all_lines);

                    let cmd = &all_lines[0].to_lowercase();
                    eprintln!("handling command:{cmd}");
                    match cmd.as_str() {
                        "ping" => {
                            if !sent_by_main {
                                conn.write_to_stream(b"+PONG\r\n");
                            }
                            eprintln!(
                                "\nIGNORING ping set by main, is replica?{}",
                                master_port.is_some()
                            );
                        }
                        "echo" => {
                            let resp = [b"+", all_lines[1].as_bytes(), b"\r\n"].concat();
                            conn.write_to_stream(&resp);
                        }

                        "set" => {
                            write_command = all_lines.clone();
                            eprintln!("after setting write commands:{:?}", write_command);
                            eprintln!("IN handle client SET,");
                            eprintln!("sent by MAIN:{sent_by_main}");

                            if all_lines.len() < 3 {
                                conn.write_to_stream(RESP_NULL);
                                continue;
                            }

                            if info_fields.get(ROLE).is_some_and(|k| k == MASTER) {
                                eprintln!(
                                    "Master starting PROPAGATION with info, {:?}",
                                    broadcast_info
                                );
                                {
                                    let mut lk = broadcast_info.lock().unwrap();
                                    lk.broadcast_command(&all_lines);
                                }
                            }

                            let k = all_lines[1].clone();
                            let v = all_lines[2].clone();

                            let mut use_time = None;
                            if all_lines.len() > 4 {
                                use_time = Some((all_lines[3].as_str(), all_lines[4].as_str()));
                            }
                            let r = handle_set(k, v, new_db, use_time);
                            if r.is_ok() && !sent_by_main {
                                eprintln!("after set writing ok to stream, curr db:{:?}", new_db);
                                conn.write_to_stream(RESP_OK);
                            }
                        }

                        /*
                         * GET SECTION
                         * */
                        "get" => {
                            if all_lines.len() < 2 {
                                conn.write_to_stream(RESP_NULL);
                                continue;
                            }
                            eprintln!("IN handle client GET, db:{:?}", new_db);
                            let get_key = &all_lines[1];
                            {
                                //eprintln!("in handle GET function before lock");
                                let mut lk = new_db.lock().expect("failed to lock db in get");
                                //eprintln!("in handle GET function locked db:{:?}", lk);
                                if let Some(res) = lk.get(&get_key) {
                                    if res.expires_at.is_some()
                                        && res.expires_at.as_ref().unwrap().is_expired()
                                    {
                                        //eprintln!("ASKING FOR EXPIRED!!?? key: {get_key}");
                                        lk.data.remove(get_key);
                                        conn.write_to_stream(crate::RESP_NULL);
                                    } else {
                                        let resp = crate::utils::get_bulk_string(&res.value);
                                        conn.write_to_stream(&resp);
                                    }
                                } else {
                                    //eprintln!("IN GET FOUND NONE");
                                    conn.write_to_stream(crate::RESP_NULL);
                                }
                            }
                        }

                        /*
                         *CONFIG
                         * */
                        "config" => {
                            let config_command = all_lines[1].to_lowercase();
                            let config_field = all_lines[2].to_lowercase();
                            let dir = dir.clone();
                            let db_filename = db_filename.clone();
                            match config_command.as_str() {
                                "get" => match config_field.as_str() {
                                    "dir" => {
                                        if let Some(dir_name) = &dir {
                                            conn.write_to_stream(
                                                &conn.format_resp_array(&[&config_field, dir_name]),
                                            );
                                            //stream.write_all(&resp).unwrap();
                                        } else {
                                            //stream.write_all(crate::RESP_NULL)?;
                                            conn.write_to_stream(crate::RESP_NULL);
                                        }
                                    }
                                    "dbfilename" => {
                                        if let Some(db_name) = &db_filename {
                                            conn.write_to_stream(
                                                &conn.format_resp_array(&[&config_field, &db_name]),
                                            );
                                        } else {
                                            conn.write_to_stream(crate::RESP_NULL);
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
                                eprintln!("FOUND FILE");
                                let file = db_filename.as_ref().unwrap();

                                let directory = dir.as_ref().unwrap();
                                eprintln!("OUND DIR");
                                // write current hashmap to rdb
                                path = Path::new(directory).join(file);
                            } else {
                                path = env::current_dir().unwrap().join("dump.rdb");
                            }

                            eprintln!("USING PATH:{:?}", &path);

                            let mut file = File::open(&path)?;
                            let mut buffer = Vec::new();
                            file.read_to_end(&mut buffer)?;

                            // eprintln!("Printin rdb as HEX");
                            // print_hex::print_hex_dump(&buffer);
                            match read_rdb_file(path) {
                                Ok(rdb) => {
                                    let ret_keys = utils::read_rdb_keys(rdb, all_lines[1].clone());

                                    //EXAMPLE: *1\r\n$3\r\nfoo\r\n
                                    let _ = conn.write_to_stream(
                                        &[b"*", ret_keys.len().to_string().as_bytes(), b"\r\n"]
                                            .concat(),
                                    );
                                    ret_keys.iter().enumerate().for_each(|(_, e)| {
                                        let _ = conn.write_to_stream(&utils::get_bulk_string(e));
                                    });
                                }
                                Err(_e) => {
                                    //eprintln!("failed to read from rdb file {:?}", e);
                                    conn.write_to_stream(RESP_NULL);
                                }
                            }
                        }

                        //SAVE
                        "save" => {
                            eprintln!("IN SAVE");
                            let mut path: PathBuf;
                            if db_filename.is_some() && dir.is_some() {
                                // create a new file path then write current hashmap to rdb
                                path = Path::new(dir.as_ref().unwrap())
                                    .join(db_filename.as_ref().unwrap());

                                eprintln!("Path:{:?}", &path);
                                let mut new_rdb = RdbFile {
                                    version: "0011".to_string(),
                                    metadata: HashMap::new(),
                                    databases: HashMap::new(),
                                };

                                new_rdb
                                    .metadata
                                    .insert("redis-version".to_string(), "6.0.16".to_string());
                                eprintln!("IN Save, using map {:?}", new_db);
                                {
                                    let lk = new_db.lock().expect("failed to lock db in save");
                                    new_rdb.databases.insert(0, lk.clone().try_into()?);
                                    eprintln!("Creating a new rdb with {:?}", new_rdb);
                                }
                                let _ = write_rdb_file(path, &new_rdb);

                                eprintln!("after SAVE writing to file");
                                conn.write_to_stream(RESP_OK);
                            } else {
                                eprintln!("Creating DUMMY in curr dir");
                                path = env::current_dir().unwrap();
                                path.push("dump.rdb");
                                //print_hex::create_dummy_rdb(&path)?;
                                conn.write_to_stream(RESP_OK);
                                // no need for data as it already mocked
                            }
                        }

                        //INFO
                        "info" => {
                            //if there is an extra key arg/"se"
                            eprintln!("IN INFO SECTION");
                            if all_lines.len() > 2 {
                                let info_key = &all_lines[1];
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
                                conn.write_to_stream(&utils::get_bulk_string(&use_resp));
                            } else {
                                eprintln!("IN INFO ELSE");
                                let mut use_val = String::new();

                                info_fields
                                    .iter()
                                    .for_each(|(k, v)| use_val.extend([k, ":", v, "\r\n"]));
                                // remove the last CRLF
                                let info_res =
                                    utils::get_bulk_string(&use_val[..use_val.len() - 2]);
                                eprintln!("RESPONSE:{:?}", String::from_utf8_lossy(&info_res));
                                conn.write_to_stream(&info_res);
                            }
                            eprintln!("AFTER INFO SECTION");
                        }

                        //REPL
                        "replconf" => {
                            eprintln!("HANDLING REPL CONF");
                            match all_lines[1].as_str() {
                                GETACK => {
                                    eprintln!("in get ack offset before - 37{},", conn.offset);
                                    let curr_offset = conn.offset - 37;
                                    conn.write_to_stream(&conn.format_resp_array(&[
                                        REPL_CONF,
                                        ACK,
                                        curr_offset.to_string().as_str(),
                                    ]));
                                }

                                DIFF => {
                                    //let diff_command = conn.format_resp_array(&[REPL_CONF, DIFF]);
                                    eprintln!("client got diff line");
                                    let fidd = conn.offset - conn.prev_offset; //- diff_command.len();
                                    conn.write_to_stream(
                                        &[b":", fidd.to_string().as_bytes(), b"\r\n"].concat(),
                                    );
                                }

                                LISTENING_PORT => {
                                    {
                                        let mut lk = broadcast_info.lock().unwrap();
                                        lk.ports.push(all_lines[2].clone());
                                    }
                                    eprintln!("after repl pushing ports:{:?}", broadcast_info);
                                    conn.write_to_stream(RESP_OK);
                                }
                                _ => {
                                    conn.write_to_stream(RESP_OK);
                                }
                            }
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
                                let replica_stream =
                                    RespConnection::new(stream.try_clone().unwrap());
                                //master_stream.is_master = true;
                                lk.connections.push(replica_stream);
                                let n = lk.connections.len();
                                let s = &mut lk.connections[n - 1];
                                //stream.write_all(&resync_response)?;
                                eprintln!("\n\n\nGOT HANDSHAKE??\n\n\n");
                                eprint!("\n\n\nSENDING RESYNC RESPONSE USING SAVED STREAM\n\n");
                                s.write_to_stream(&resync_response);

                                eprint!("\n\n\nSENDING RDB USING SAVED STREAM:{:?}\n\n", s);
                                let dummy_rdb_path = env::current_dir().unwrap().join("empty.rdb");
                                // let dummy_rdb_path =
                                //     env::current_dir().unwrap().join("dump_dummy.rdb");
                                // create_dummy_rdb(&dummy_rdb_path.as_path())
                                //     .expect("FAILED TO MAKE DUMMY RDB");
                                if let Ok(response_rdb_bytes) = fs::read(dummy_rdb_path) {
                                    eprintln!("IN MASTER SENDING RDB");
                                    //stream
                                    eprintln!("writing rdb len {}", response_rdb_bytes.len());
                                    //print_hex_dump(&response_rdb_bytes);

                                    s.write_to_stream(
                                        &[
                                            b"$",
                                            response_rdb_bytes.len().to_string().as_bytes(),
                                            b"\r\n",
                                        ]
                                        .concat(),
                                    );

                                    s.write_to_stream(&response_rdb_bytes)
                                }
                            };
                            eprintln!(
                                "AFTER FULL RESYNC adding connection to broadcast info, new {:?}",
                                broadcast_info
                            );
                        }

                        "wait" => {
                            let mut all_repls = Vec::new();
                            {
                                let lk = broadcast_info.lock().unwrap();
                                lk.connections.iter().enumerate().for_each(|(i, c)| {
                                    all_repls
                                        .push((c.stream.try_clone().unwrap(), lk.ports[i].clone()))
                                });
                            }
                            let num_required = all_lines[1].parse::<usize>().unwrap();

                            let wait_for_ms =
                                Duration::from_millis(all_lines[2].parse::<u64>().unwrap());
                            let num_repls = all_repls.len();

                            let ack_req =
                                conn.format_resp_array(&[crate::REPL_CONF, crate::GETACK, "*"]);
                            //let wait_pool = ThreadPool::new(lk.connections.len());

                            //let s = String::from_utf8(tm.into()).unwrap();
                            //let count_ack = Arc::new(Mutex::new(0));
                            let mut ack_threads = Vec::new();
                            /*
                             * For each replica spawn a thread and keep the join handle,
                             * thread returns 0 if timeout, 1 if successful ack
                             * use own custom ack request that is ignored by the offset
                             * store the set command received, and each conn keps a prev offset
                             * compare diff in offset if the same as the size, will mean an
                             * acknowledgement of the command.
                             * */

                            eprintln!(
                                "checking n={} replicas for write command:{:?}",
                                num_repls, write_command
                            );

                            if !write_command.is_empty() {
                                for replica in &mut all_repls {
                                    //let c = Arc::clone(&count_ack);
                                    let start_time = SystemTime::now();
                                    let end_time = start_time + wait_for_ms;
                                    let mut repl_stream = replica.0.try_clone().unwrap();
                                    //let mut repl_stream =
                                    //    TcpStream::connect(format!("127.0.0.1:{}", replica.1))
                                    //        .unwrap();
                                    //repl_stream.set_nonblocking(true).unwrap();
                                    // let diff_req =
                                    //     conn.format_resp_array(&[crate::REPL_CONF, crate::DIFF]);
                                    let command_len = conn
                                        .format_resp_array(
                                            write_command
                                                .iter()
                                                .map(|e| e.as_str())
                                                .collect::<Vec<&str>>()
                                                .iter()
                                                .as_slice(),
                                        )
                                        .len();

                                    let expected_byte_len =
                                        format!(":{command_len}\r\n").as_bytes().len();
                                    let arq = ack_req.clone();
                                    //wait_pool.execute(move || loop {
                                    let res = thread::spawn(move || {
                                        // let mut repl_stream =
                                        //     TcpStream::connect(repl_stream.peer_addr().unwrap())
                                        //         .unwrap();
                                        repl_stream.write_all(&arq).unwrap();
                                        repl_stream.set_nonblocking(false).unwrap();
                                        let mut buf = Vec::with_capacity(expected_byte_len);
                                        let _ = repl_stream.set_read_timeout(Some(wait_for_ms));
                                        let _ = repl_stream.read_exact(&mut buf);
                                        repl_stream.set_nonblocking(true).unwrap();
                                        eprintln!("got resp:{:?}", String::from_utf8_lossy(&buf));
                                        let res = String::from_utf8(buf.into()).unwrap();
                                        if !res.is_empty() {
                                            let res = res.as_str().chars().nth(1).unwrap() as usize;
                                            eprintln!("\n\nIN THREAD got size:{res}");
                                            //if res == command_len {
                                            return 1;
                                            //}
                                        }

                                        // if SystemTime::now() > end_time {
                                        //     return 0;
                                        // }
                                        //   thread::sleep(wait_for_ms / 10);
                                        0
                                    });
                                    ack_threads.push(res);
                                }

                                //sleep(Duration::from_millis(100));
                                //sleep(wait_for_ms);
                                let mut final_count = 0;
                                for handle in ack_threads {
                                    let res = handle.join().expect("failed joining ack threads");
                                    final_count += res;
                                    if final_count == num_required {
                                        break;
                                    }
                                }

                                eprintln!("\n\nRESPONDING WITH {final_count} ACKS\n\n");
                                conn.write_to_stream(
                                    &[
                                        ":".as_bytes(),
                                        //&*(count_ack.lock().unwrap()).to_string().as_bytes(),
                                        final_count.to_string().as_bytes(),
                                        "\r\n".as_bytes(),
                                    ]
                                    .concat(),
                                );
                            } else {
                                conn.write_to_stream(":0\r\n".as_bytes());
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
            }
            Ok(None) => {
                //std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => {
                eprintln!("Connection error: {}", e);
                break;
            }
        }
    }
    Ok(())
}
