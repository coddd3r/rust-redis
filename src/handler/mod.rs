use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io::{prelude::*, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::{self, sleep};
use std::time::{Duration, SystemTime};

use crate::entry_stream::entry_utils::{get_all_stream_names, get_xread_resp_array};
use crate::entry_stream::RedisEntryStream;
use crate::redis_channel::Channel;
use crate::redis_connection::broadcast_info::BroadCastInfo;
use crate::redis_connection::RedisConnection;
use crate::redis_database::{
    read_rdb_file, write_rdb_file, RdbError, RdbFile, RedisDatabase, RedisValue,
};
use crate::redis_list::RedisList;
use crate::redis_sorted_set::RedisSortedSet;
use crate::utils::{get_port, get_redis_int, get_resp_from_string, read_rdb_keys};

use crate::constants::*;

use crate::handler::command_handlers::handle_set;
use crate::utils::get_bulk_string;

mod command_handlers;

pub fn handle_connection(
    //stream: Arc<Mutex<TcpStream>>,
    stream: TcpStream,
    dir: Option<String>,
    db_filename: Option<String>,
    info_fields: HashMap<String, String>,
    broadcast_info: Arc<Mutex<BroadCastInfo>>,
    replica_port: &Option<&str>,
    master_port: &Option<String>,
    new_db: Arc<Mutex<RedisDatabase>>,
    entry_streams: Arc<Mutex<HashMap<String, RedisEntryStream>>>,
    lists_map: Arc<Mutex<HashMap<String, RedisList>>>,
    channels_db: Arc<Mutex<HashMap<String, Channel>>>,
    sets_map: Arc<Mutex<HashMap<String, RedisSortedSet>>>, //subscribers_db: Arc<Mutex<HashMap<String, Subscriber>>>,
) -> Result<(), Box<dyn Error>> {
    eprintln!(
        "handling_connection, master_port:{:?}, stream port:{:?}",
        master_port,
        get_port(&stream)
    );

    let sent_by_main = master_port.is_some() && get_port(&stream) == *master_port;

    let mut conn = RedisConnection::new(stream.try_clone().unwrap());

    if sent_by_main {
        conn.is_master = true;
        eprintln!("\n\n\n\nHANDLING HANDSHAKE\n\n\n\n\n");
        conn.write_to_stream(&conn.format_resp_array(&["PING"]).as_bytes());
        let _res = conn.try_read_command();
        sleep(Duration::from_millis(10));
        //eprintln!("Read result: {:?}", res);

        let use_bytes = conn.format_resp_array(&[REPL_CONF, LISTENING_PORT, replica_port.unwrap()]);
        conn.write_to_stream(&use_bytes.as_bytes());
        let _res = conn.try_read_command();
        sleep(Duration::from_millis(10));

        conn.write_to_stream(
            &conn
                .format_resp_array(&[REPL_CONF, "capa", "psync2"])
                .as_bytes(),
        );
        let _res = conn.try_read_command();
        sleep(Duration::from_millis(10));

        conn.write_to_stream(&conn.format_resp_array(&[PSYNC, "?", "-1"]).as_bytes());
        //ignore the last sent after psync
    }

    let mut write_command: Vec<_> = Vec::new();

    let mut all_multi_commands = Vec::new();
    let mut is_exec_mode = false;
    let mut hold_all_exec_reponse: Vec<String> = Vec::new();
    loop {
        match conn.try_read_command() {
            Ok(Some(mut commands)) => {
                eprintln!("ALL COMMANDS:{:?}", commands);

                let mut response_to_write = String::new();
                let mut exec_present = false;
                let mut discard_present = false;
                let mut discard_index: usize = 0;
                commands.iter().flatten().enumerate().for_each(|(i, s)| {
                    exec_present = s.to_lowercase() == "exec";
                    if s.to_uppercase() == "DISCARD" {
                        discard_index = i;
                        discard_present = true;
                    }
                });

                if conn.multi_waiting && discard_present {
                    conn.multi_waiting = false;
                    commands = commands[discard_index + 1..].to_vec();
                    all_multi_commands = Vec::new();
                    conn.write_to_stream(RESP_OK.as_bytes());
                    //DO NOT continue in case some commands read from buffer after discard
                } else if conn.multi_waiting && exec_present {
                    all_multi_commands.extend(commands);
                    commands = all_multi_commands;
                    all_multi_commands = Vec::new();
                    is_exec_mode = true;
                    //eprintln!("\n\nEXEC MODE\n\n");
                    //eprintln!("exec commands:{:?}", commands);
                    conn.multi_waiting = false;
                } else if conn.multi_waiting {
                    all_multi_commands.extend(commands);
                    conn.write_to_stream(QUEUED_RESP.as_bytes());
                    continue;
                }

                for all_lines in commands {
                    if all_lines.is_empty() {
                        continue;
                    }

                    let cmd = &all_lines[0];

                    if conn.in_sub_mode && !ALLOWED_SUB_COMMANDS.contains(&cmd.as_str()) {
                        eprintln!("IN SUB MODE IGNORING COMMAND:{:?}", all_lines);
                        let use_err = format!("-ERR Can't execute '{cmd}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n");
                        conn.write_to_stream(use_err.as_bytes());
                        continue;
                    }
                    //eprintln!("handling command:{cmd}");
                    match cmd.to_lowercase().as_str() {
                        "command" => {
                            //eprintln!("INITIATION, no command");
                            return Ok(());
                        }
                        "ping" => {
                            if conn.in_sub_mode {
                                response_to_write = conn.format_resp_array(&["pong", ""]);
                            } else if !sent_by_main {
                                response_to_write = PONG_RESPONSE.to_string();
                            }
                        }
                        "echo" => {
                            //let resp = [b"+", all_lines[1].as_bytes(), b"\r\n"].concat();
                            let resp = format!("+{}\r\n", all_lines[1]);
                            response_to_write = resp.to_string()
                        }

                        "set" => {
                            write_command = all_lines.clone();
                            //eprintln!("after setting write commands:{:?}", write_command);
                            //eprintln!("IN handle client SET,");
                            //eprintln!("sent by MAIN:{sent_by_main}");

                            if all_lines.len() < 3 {
                                response_to_write = RESP_NULL.to_string();
                                continue;
                            }

                            if info_fields.get(ROLE).is_some_and(|k| k == MASTER) {
                                //eprintln!(
                                //    "Master starting PROPAGATION with info, {:?}",
                                //    broadcast_info
                                //);
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
                            let r = handle_set(k, v, &new_db, use_time);
                            if r.is_ok() && !sent_by_main {
                                //eprintln!("after set writing ok to stream, curr db:{:?}", new_db);
                                response_to_write = RESP_OK.to_string()
                            }
                        }

                        /*
                         * GET SECTION
                         * */
                        "get" => {
                            if all_lines.len() < 2 {
                                response_to_write = RESP_NULL.to_string();
                                continue;
                            }
                            //eprintln!("IN handle client GET, db:{:?}", new_db);
                            let get_key = &all_lines[1];
                            {
                                ////eprintln!("in handle GET function before lock");
                                let mut lk = new_db.lock().expect("failed to lock db in get");
                                ////eprintln!("in handle GET function locked db:{:?}", lk);
                                if let Some(res) = lk.get(&get_key) {
                                    if res.expires_at.is_some()
                                        && res.expires_at.as_ref().unwrap().is_expired()
                                    {
                                        ////eprintln!("ASKING FOR EXPIRED!!?? key: {get_key}");
                                        lk.data.remove(get_key);
                                        response_to_write = RESP_NULL.to_string();
                                    } else {
                                        let resp = get_bulk_string(&res.value);
                                        response_to_write = resp;
                                    }
                                } else {
                                    eprintln!("IN GET FOUND NONE");
                                    response_to_write = RESP_NULL.to_string();
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
                                            response_to_write =
                                                conn.format_resp_array(&[&config_field, dir_name])
                                        } else {
                                            response_to_write = RESP_NULL.to_string();
                                        }
                                    }
                                    "dbfilename" => {
                                        if let Some(db_name) = &db_filename {
                                            response_to_write =
                                                conn.format_resp_array(&[&config_field, &db_name])
                                        } else {
                                            response_to_write = RESP_NULL.to_string();
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
                                //eprintln!("FOUND FILE");
                                let file = db_filename.as_ref().unwrap();

                                let directory = dir.as_ref().unwrap();
                                //eprintln!("FOUND DIR");
                                // write current hashmap to rdb
                                path = Path::new(directory).join(file);
                            } else {
                                path = env::current_dir().unwrap().join("dump.rdb");
                            }

                            //eprintln!("USING PATH:{:?}", &path);

                            let mut file = File::open(&path)?;
                            let mut buffer = Vec::new();
                            file.read_to_end(&mut buffer)?;

                            // //eprintln!("Printin rdb as HEX");
                            // print_hex::print_hex_dump(&buffer);
                            match read_rdb_file(path) {
                                Ok(rdb) => {
                                    let ret_keys = read_rdb_keys(rdb, all_lines[1].clone());

                                    //EXAMPLE: *1\r\n$3\r\nfoo\r\n
                                    response_to_write = conn.format_resp_array(
                                        ret_keys
                                            .iter()
                                            .map(|e| e.as_str())
                                            .collect::<Vec<&str>>()
                                            .as_slice(),
                                    )
                                }
                                Err(e) => {
                                    eprintln!("failed to read from rdb file {:?}", e);
                                    response_to_write = RESP_NULL.to_string();
                                }
                            }
                        }

                        //SAVE
                        "save" => {
                            //eprintln!("IN SAVE");
                            let mut path: PathBuf;
                            if db_filename.is_some() && dir.is_some() {
                                // create a new file path then write current hashmap to rdb
                                path = Path::new(dir.as_ref().unwrap())
                                    .join(db_filename.as_ref().unwrap());

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
                                response_to_write = RESP_OK.to_string();
                            } else {
                                //eprintln!("Creating DUMMY in curr dir");
                                path = env::current_dir().unwrap();
                                path.push("dump.rdb");
                                //print_hex::create_dummy_rdb(&path)?;
                                response_to_write = RESP_OK.to_string();
                                // no need for data as it already mocked
                            }
                        }

                        //INFO
                        "info" => {
                            //if there is an extra key arg/"se"
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
                                //eprintln!("INFO RESPONSE:{:?}", use_resp);
                                response_to_write = get_bulk_string(&use_resp);
                            } else {
                                //eprintln!("IN INFO ELSE");
                                let mut use_val = String::new();

                                info_fields
                                    .iter()
                                    .for_each(|(k, v)| use_val.extend([k, ":", v, "\r\n"]));
                                // remove the last CRLF
                                let info_res = get_bulk_string(&use_val[..use_val.len() - 2]);
                                //eprintln!("RESPONSE:{:?}", &info_res);
                                response_to_write = info_res;
                            }
                            //eprintln!("AFTER INFO SECTION");
                        }

                        //REPL
                        "replconf" => {
                            //eprintln!("HANDLING REPL CONF");
                            match all_lines[1].as_str() {
                                GETACK => {
                                    eprintln!("in get ack offset before - 37 is:{},", conn.offset);
                                    let curr_offset = conn.offset - 37;
                                    response_to_write = conn.format_resp_array(&[
                                        REPL_CONF,
                                        ACK,
                                        curr_offset.to_string().as_str(),
                                    ]);
                                }

                                LISTENING_PORT => {
                                    {
                                        let mut lk = broadcast_info.lock().unwrap();
                                        lk.ports.push(all_lines[2].clone());
                                    }
                                    //eprintln!("after repl pushing ports:{:?}", broadcast_info);
                                    response_to_write = RESP_OK.to_string();
                                }

                                ACK => {
                                    let mut lk = broadcast_info.lock().unwrap();
                                    //eprintln!("got command in ACK, num waiting for:{}, wiaiting until:{:?}",lk.num_waiting_for, waiting_until);
                                    if lk.num_waiting_for > 0 {
                                        //eprintln!("ADD TO ACK");
                                        lk.num_acks += 1;
                                    }
                                }

                                _ => {
                                    response_to_write = RESP_OK.to_string();
                                    //eprintln!("WROTE ok to other replconf");
                                }
                            }
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
                                    RedisConnection::new(stream.try_clone().unwrap());
                                //master_stream.is_master = true;
                                lk.connections.push(replica_stream);
                                let n = lk.connections.len();
                                let s = &mut lk.connections[n - 1];
                                //stream.write_all(&resync_response)?;
                                eprintln!("\n\n\n MASTER GOT HANDSHAKE??\n\n\n");
                                //eprint!("\n\n\nSENDING RESYNC RESPONSE USING SAVED STREAM\n\n");
                                s.write_to_stream(&resync_response);

                                //eprint!("\n\n\nSENDING RDB USING SAVED STREAM:{:?}\n\n", s);
                                let dummy_rdb_path = env::current_dir().unwrap().join("empty.rdb");
                                // let dummy_rdb_path =
                                //     env::current_dir().unwrap().join("dump_dummy.rdb");
                                // create_dummy_rdb(&dummy_rdb_path.as_path())
                                //     .expect("FAILED TO MAKE DUMMY RDB");
                                if let Ok(response_rdb_bytes) = fs::read(dummy_rdb_path) {
                                    //eprintln!("IN MASTER SENDING RDB");
                                    //eprintln!("writing rdb len {}", response_rdb_bytes.len());
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
                            //eprintln!(
                            //    "AFTER FULL RESYNC adding connection to broadcast info, new {:?}",
                            //    broadcast_info
                            //);
                        }

                        "wait" => {
                            let mut all_repls = Vec::new();
                            let num_required = all_lines[1].parse::<usize>().unwrap();
                            let wait_for_ms =
                                Duration::from_millis(all_lines[2].parse::<u64>().unwrap());
                            {
                                let mut lk = broadcast_info.lock().unwrap();
                                lk.num_waiting_for = num_required;
                                lk.waiting_until = SystemTime::now() + wait_for_ms; //eprintln!("setting num waiting for:{}", lk.num_waiting_for);
                                lk.connections.iter().enumerate().for_each(|(i, c)| {
                                    all_repls
                                        .push((c.stream.try_clone().unwrap(), lk.ports[i].clone()))
                                });
                            }

                            let num_repls = all_repls.len();
                            let ack_req = conn.format_resp_array(&[REPL_CONF, GETACK, "*"]);

                            //eprintln!(
                            //    "checking n={} replicas, waiting for{:?}",
                            //    num_repls, wait_for_ms
                            //);

                            let mut acq_threads = Vec::new();
                            if !write_command.is_empty() {
                                for replica in &mut all_repls {
                                    let mut repl_stream = replica.0.try_clone().unwrap();
                                    let arq = ack_req.clone();
                                    let res = thread::spawn(move || {
                                        repl_stream.write_all(&arq.as_bytes()).unwrap();
                                    });
                                    acq_threads.push(res);
                                }
                                acq_threads.into_iter().for_each(|e| {
                                    let _ = e.join();
                                });

                                sleep(Duration::from(wait_for_ms));
                                let mut lk = broadcast_info.lock().unwrap();
                                if lk.num_waiting_for > 0
                                    && (SystemTime::now() > lk.waiting_until
                                        || lk.num_acks == lk.num_waiting_for)
                                {
                                    //eprintln!("\n\nRESPONDING WITH {} ACKS\n\n", lk.num_acks);
                                    // conn.write_to_stream(
                                    //     &[
                                    //         ":".as_bytes(),
                                    //         lk.num_acks.to_string().as_bytes(),
                                    //         "\r\n".as_bytes(),
                                    //     ]
                                    //     .concat(),
                                    // );

                                    response_to_write = get_redis_int(lk.num_acks as i32);
                                    lk.num_acks = 0;
                                    lk.num_waiting_for = 0;
                                }
                                std::mem::drop(lk);

                                //eprintln!("after threads");
                            } else {
                                response_to_write = format!(":{}\r\n", num_repls);
                            }
                        }

                        "type" => {
                            let key = &all_lines[1];
                            //eprintln!(
                            //    "key:{key}, currentdb:{:?}, current entry_strem{:?}",
                            //    new_db, entry_streams
                            //);

                            if new_db.lock().unwrap().get(key).is_some() {
                                response_to_write = STRING.to_string();
                            } else if entry_streams.lock().unwrap().get(key).is_some() {
                                response_to_write = conn.get_simple_str("stream");
                            } else {
                                response_to_write = NONE_TYPE.to_string();
                            }
                        }

                        "xadd" => {
                            let stream_name = all_lines[1].clone();
                            let stream_id = all_lines[2].clone();
                            //let (k, v) = (all_lines[3].clone(), all_lines[4].clone());
                            //eprintln!("handling x_add with key:{stream_name}, id:{stream_id}, k:{k}, v:{v}");

                            let mut lk = entry_streams.lock().unwrap();
                            let curr_stream =
                                lk.entry(stream_name).or_insert(RedisEntryStream::new());

                            let mut use_vec = Vec::new();
                            // map key values to each other in a tuple
                            for i in 3..all_lines.len() {
                                if i % 2 == 0 {
                                    continue;
                                }
                                use_vec.push((all_lines[i].clone(), all_lines[i + 1].clone()))
                            }

                            let res = curr_stream.handle_add(&stream_id.as_str(), use_vec);

                            response_to_write = res;
                        }

                        "xrange" => {
                            let stream_name = all_lines[1].clone();
                            let start = all_lines[2].clone();
                            let end = all_lines[3].clone();
                            //eprintln!(
                            //    "handling XRANGE with key:{stream_name}, start:{start}, end:{end}"
                            //);
                            {
                                let mut lk = entry_streams.lock().unwrap();
                                let curr_stream =
                                    lk.entry(stream_name).or_insert(RedisEntryStream::new());

                                response_to_write = curr_stream.get_from_range(&start, &end);
                            }
                        }

                        "xread" => {
                            let block = all_lines[1] == "block";
                            let block_start_time = SystemTime::now();
                            // TODO: check for invalid times
                            let mut time_to_block_for: Duration = Duration::from_millis(0);
                            let all_streams;

                            let time_str = all_lines[2].parse::<u64>();
                            let mut full_block = false;
                            if block {
                                //TODO: IF TIME STRING IS 0 add a blcoked stream conn clone to
                                //waiting streams

                                //eprintln!("\n\n\nGOT BLOCK with time{}\n\n\n", all_lines[2]);
                                let actual_time = time_str.as_ref().unwrap();
                                full_block = actual_time == &0;
                                time_to_block_for =
                                    Duration::from_millis(*time_str.as_ref().unwrap());
                                if !full_block {
                                    sleep(time_to_block_for);
                                } else {
                                    //eprintln!("\n\nFULL BLOCK\n\n");
                                }
                                all_streams = get_all_stream_names(&all_lines[4..]);
                            } else {
                                all_streams = get_all_stream_names(&all_lines[2..]);
                            }

                            let mut final_res = Vec::new();
                            let mut lk = entry_streams.lock().unwrap();
                            for (stream_name, start) in all_streams {
                                let curr_stream = lk
                                    .entry(stream_name.clone())
                                    .or_insert(RedisEntryStream::new());
                                //eprintln!("curr stream{:?}", curr_stream);

                                //eprintln!("running xread for stream_name{:?}", &stream_name);
                                if full_block {
                                    //eprintln!("\n\n\nADDING FULL BLOCK\n\n\n");
                                    curr_stream.waiting_streams.insert(
                                        stream_name.clone(),
                                        conn.stream.try_clone().unwrap(),
                                    );
                                } else {
                                    //eprintln!("\n\n\nNOT full block\n\n\n");
                                    let res = {
                                        if block {
                                            curr_stream.block_xread(
                                                &stream_name,
                                                block_start_time,
                                                time_to_block_for,
                                                &start,
                                            )
                                        } else {
                                            curr_stream.xread_range(&stream_name, &start)
                                        }
                                    };
                                    if res.is_some() {
                                        final_res.push(res.unwrap())
                                    }
                                }
                            }

                            let full_stream_bytes = get_xread_resp_array(&final_res);
                            //eprintln!("FINAL xread res:{:?}", full_stream_bytes);
                            if !full_block {
                                response_to_write = full_stream_bytes;
                            }
                        }

                        "incr" => {
                            let mut lk = new_db.lock().unwrap();
                            let key = all_lines[1].clone();

                            let rv = lk.data.entry(key).or_insert(RedisValue {
                                value: "0".to_string(),
                                expires_at: None,
                            });

                            if let Ok(val) = rv.value.parse::<i32>() {
                                let new_val = val + 1;
                                rv.value = new_val.to_string();
                                response_to_write = get_redis_int(new_val);
                            } else {
                                response_to_write = NOT_INT_ERROR.to_string();
                            }
                        }

                        "multi" => {
                            conn.multi_waiting = true;
                            conn.write_to_stream(RESP_OK.as_bytes());
                            continue;
                        }

                        "exec" => {
                            if is_exec_mode {
                                conn.multi_waiting = false;
                                is_exec_mode = false;
                                let mut exec_resp = format!("*{}\r\n", hold_all_exec_reponse.len());
                                hold_all_exec_reponse
                                    .iter()
                                    .for_each(|e| exec_resp.push_str(&e));
                                //conn.write_to_stream(exec_resp.as_bytes());
                                response_to_write = exec_resp;
                                hold_all_exec_reponse = Vec::new();
                            } else {
                                response_to_write = EXEC_WITHOUT_MULTI.to_string();
                            }
                        }
                        "discard" => {
                            conn.write_to_stream(b"-ERR DISCARD without MULTI\r\n");
                        }

                        "rpush" => {
                            let key = &all_lines[1];
                            let mut lk = lists_map.lock().unwrap();
                            let use_list =
                                lk.entry(key.clone()).or_insert(RedisList::new(key.clone()));
                            all_lines[2..].iter().for_each(|e| {
                                use_list.values.push(e.clone());
                            });

                            let num_vals = use_list.values.len();
                            use_list.check_waiting_streams();

                            response_to_write = get_redis_int(num_vals as i32);
                        }

                        "lpush" => {
                            let key = &all_lines[1];
                            let mut lk = lists_map.lock().unwrap();
                            let use_list =
                                lk.entry(key.clone()).or_insert(RedisList::new(key.clone()));
                            all_lines[2..].iter().for_each(|e| {
                                use_list.values.splice(0..0, [e.clone()]);
                            });

                            let num_vals = use_list.values.len();
                            use_list.check_waiting_streams();

                            response_to_write = get_redis_int(num_vals as i32);
                        }

                        "lrange" => {
                            let key = &all_lines[1];
                            let mut start = all_lines[2].parse::<i32>().unwrap();
                            let mut end = all_lines[3].parse::<i32>().unwrap();

                            let lk = lists_map.lock().unwrap();
                            let search_opt = lk.get(key);
                            match search_opt {
                                Some(use_list) => {
                                    let list_size = use_list.values.len() as i32;
                                    if start < 0 {
                                        if list_size + start < 0 {
                                            start = 0
                                        } else {
                                            start = list_size + start
                                        };
                                    }
                                    if end < 0 {
                                        end = list_size + end;
                                    }
                                    eprintln!("HANDLING lrange with start:{start}, end:{end}");

                                    if start >= list_size || start > end || start < 0 || end < 0 {
                                        response_to_write = EMPTY_ARRAY.to_string();
                                    } else {
                                        if end >= list_size {
                                            end = list_size - 1;
                                        }
                                        let start = start as usize;
                                        let end = end as usize;
                                        response_to_write = conn.format_resp_array(
                                            use_list.values[start..end + 1]
                                                .iter()
                                                .map(|e| e.as_str())
                                                .collect::<Vec<&str>>()
                                                .as_slice(),
                                        )
                                    }
                                }
                                None => {
                                    response_to_write = EMPTY_ARRAY.to_string();
                                }
                            }
                        }

                        "llen" => {
                            let key = &all_lines[1];
                            let lk = lists_map.lock().unwrap();
                            let search_opt = lk.get(key);
                            match search_opt {
                                Some(use_list) => {
                                    response_to_write = get_redis_int(use_list.values.len() as i32);
                                }
                                None => response_to_write = get_redis_int(0),
                            }
                        }

                        "lpop" => {
                            eprintln!("in lpop");
                            let key = &all_lines[1];
                            let num_to_remove = {
                                if all_lines.len() > 2 {
                                    all_lines[2].parse().unwrap()
                                } else {
                                    1
                                }
                            };

                            let mut lk = lists_map.lock().unwrap();
                            let search_opt = lk.get_mut(key);
                            match search_opt {
                                Some(use_list) => {
                                    if num_to_remove == 1 {
                                        response_to_write =
                                            get_bulk_string(&use_list.values.remove(0));
                                    } else {
                                        let mut use_nums = Vec::new();
                                        for _ in 0..num_to_remove {
                                            use_nums.push(use_list.values.remove(0));
                                        }
                                        response_to_write =
                                            get_resp_from_string(use_nums.as_slice());
                                    }
                                }
                                None => response_to_write = RESP_NULL.to_string(),
                            }
                        }

                        "blpop" => {
                            let key = all_lines[1].clone();
                            let blocking_time = all_lines[2].parse::<f64>().unwrap();
                            let mut timed_block = None;

                            // scope to make sure lock is dropped after match
                            {
                                let mut lk = lists_map.lock().unwrap();
                                let use_list =
                                    lk.entry(key.clone()).or_insert(RedisList::new(key.clone()));

                                if blocking_time == 0.0 && !use_list.values.is_empty() {
                                    response_to_write = get_resp_from_string(&[
                                        key.clone(),
                                        use_list.values.remove(0),
                                    ]);
                                } else if blocking_time == 0.0 {
                                    use_list
                                        .blocking_pop_streams
                                        .push(conn.stream.try_clone().unwrap());
                                } else {
                                    let blocking_ms = (blocking_time * 1_000.0) as u64;
                                    let blocking_dur = Duration::from_millis(blocking_ms);
                                    timed_block =
                                        Some((conn.stream.try_clone().unwrap(), blocking_dur));
                                }
                            }

                            if let Some((mut st, blocking_dur)) = timed_block {
                                let blocking_until = SystemTime::now() + blocking_dur;
                                let use_lkd_db = Arc::clone(&lists_map);
                                thread::spawn(move || loop {
                                    //sleep(blocking_dur / 5);
                                    sleep(Duration::from_millis(50));
                                    if SystemTime::now() > blocking_until {
                                        let _ = st.write_all(RESP_NULL.as_bytes());
                                        break;
                                    }
                                    let mut lk = use_lkd_db.lock().unwrap();
                                    let use_list = lk
                                        .entry(key.clone())
                                        .or_insert(RedisList::new(key.clone()));
                                    if !use_list.values.is_empty() {
                                        let _ = st.write_all(
                                            get_resp_from_string(&[
                                                key.clone(),
                                                use_list.values.remove(0),
                                            ])
                                            .as_bytes(),
                                        );
                                        break;
                                    }
                                });
                            }
                        }

                        "subscribe" => {
                            eprintln!("received subscription!");
                            let chan_name = &all_lines[1];
                            let mut chan_lk = channels_db.lock().unwrap();
                            let chan = chan_lk
                                .entry(chan_name.clone())
                                .or_insert(Channel::new(chan_name));
                            //let add_chan = chan_lk.get(&chan_name.clone()).unwrap();
                            //if let Some(port) = get_port(&conn.stream) {
                            //let mut sub_lk = subscribers_db.lock().unwrap();
                            // let subber =
                            //     sub_lk.entry(port.clone()).or_insert(Subscriber::new(port));
                            //TODO: check if channel has the subber first before adding

                            if !chan
                                .subscribers
                                .iter()
                                .any(|sb| get_port(sb) == get_port(&conn.stream))
                            {
                                eprintln!("different subber");
                                //subber.channel_count += 1;
                                //conn.subbed_channels.push(chan_name.clone());
                                conn.num_channels += 1;
                                let num_chans = (conn.num_channels + 0) as i32;
                                chan.subscribers.push(conn.stream.try_clone().unwrap());
                                response_to_write = format!(
                                    "*3\r\n{}{}{}",
                                    get_bulk_string("subscribe"),
                                    get_bulk_string(chan_name),
                                    get_redis_int(num_chans)
                                );
                                conn.in_sub_mode = true;
                            } else {
                                eprintln!("\n\nALREADY SUBBED!!\n");
                            }
                            //}
                        }

                        "publish" => {
                            let chan_name = &all_lines[1];
                            let msg = &all_lines[2];
                            let lk = channels_db.lock().unwrap();
                            if let Some(curr_chan) = lk.get(chan_name) {
                                let num_subs = curr_chan.subscribers.len();
                                response_to_write = get_redis_int(num_subs as i32);

                                for mut st in &curr_chan.subscribers {
                                    let _ = st.write_all(
                                        conn.format_resp_array(&[
                                            "message",
                                            chan_name,
                                            msg.as_str(),
                                        ])
                                        .as_bytes(),
                                    );
                                }
                            }
                        }

                        "unsubscribe" => {
                            let chan_name = &all_lines[1];
                            let mut lk = channels_db.lock().unwrap();
                            if let Some(curr_chan) = lk.get_mut(chan_name) {
                                for i in 0..curr_chan.subscribers.len() {
                                    let curr_port = get_port(&conn.stream);
                                    if get_port(&curr_chan.subscribers[i]) == curr_port {
                                        curr_chan.subscribers.remove(i);
                                        conn.num_channels -= 1;
                                        let num_chans = (conn.num_channels + 0) as i32;
                                        response_to_write = format!(
                                            "*3\r\n{}{}{}",
                                            get_bulk_string("unsubscribe"),
                                            get_bulk_string(chan_name),
                                            get_redis_int(num_chans)
                                        );

                                        break;
                                    }
                                }
                            }
                        }

                        "zadd" => {
                            let set_name = &all_lines[1];
                            let score = &all_lines[2];
                            let name = &all_lines[3];
                            let mut lk = sets_map.lock().unwrap();
                            let curr_set =
                                lk.entry(set_name.clone()).or_insert(RedisSortedSet::new());
                            let is_new = curr_set.insert(&score, &name);
                            eprintln!("\n\nSETS MAP LOCK RELEASED\n\n");
                            let use_num = {
                                if is_new {
                                    1
                                } else {
                                    0
                                }
                            };
                            response_to_write = get_redis_int(use_num);
                        }

                        "zrank" => {
                            let set_name = &all_lines[1];
                            let member_name = &all_lines[2];
                            let lk = sets_map.lock().unwrap();
                            if let Some(found_set) = lk.get(set_name) {
                                if let Some(rank_res) = found_set.rank(member_name) {
                                    response_to_write = get_redis_int(rank_res as i32)
                                } else {
                                    response_to_write = RESP_NULL.to_string();
                                }
                            } else {
                                response_to_write = RESP_NULL.to_string();
                            }
                        }

                        "zrange" => {
                            let set_name = &all_lines[1];
                            let start = all_lines[2].parse::<i32>().unwrap();
                            let end = all_lines[3].parse::<i32>().unwrap();
                            let lk = sets_map.lock().unwrap();

                            if let Some(found_set) = lk.get(set_name) {
                                response_to_write = found_set.range_resp_array(start, end)
                            } else {
                                response_to_write = EMPTY_ARRAY.into()
                            }
                        }

                        "zcard" => {
                            let set_name = &all_lines[1];
                            let lk = sets_map.lock().unwrap();

                            if let Some(found_set) = lk.get(set_name) {
                                response_to_write = get_redis_int(found_set.len() as i32);
                            } else {
                                response_to_write = ZERO_INT.into();
                            }
                        }

                        _unrecognized_cmd => {
                            return Err(Box::new(RdbError::UnsupportedFeature(
                                "UNRECOGNIZED COMMAND",
                            )))
                        }
                    }
                    /*
                     * HANDLE COMMAND RESPONSES
                     */
                    if is_exec_mode {
                        hold_all_exec_reponse.push(response_to_write.clone());
                        //eprintln!("EXEC MODE!! with resps:{:?}", hold_all_exec_reponse);
                        continue;
                    } else if !response_to_write.is_empty() {
                        conn.write_to_stream(response_to_write.as_bytes());
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!("Connection error: {}", e);
                break;
            }
        }
    }
    Ok(())
}
