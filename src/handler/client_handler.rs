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

use crate::resp_parser::BroadCastInfo;

struct ClientHandler {
    stream: TcpStream,
    dir: Option<String>,
    db_filename: Option<String>,
    info_fields: HashMap<String, String>,
    broadcast_info: Arc<std::sync::Mutex<BroadCastInfo>>,
    master_port: Option<String>,
    new_db: Arc<Mutex<RedisDatabase>>,
}
impl ClientHandler {
    fn new(
        mut stream: TcpStream,
        dir: Option<String>,
        db_filename: Option<String>,
        info_fields: HashMap<String, String>,
        broadcast_info: Arc<std::sync::Mutex<BroadCastInfo>>,
        master_port: Option<String>,
        new_db: Arc<Mutex<RedisDatabase>>,
    ) -> Self {
        Self {
            stream,
            dir,
            db_filename,
            info_fields,
            broadcast_info,
            master_port,
            new_db,
        }
    }

    pub fn execute_command(cmd: &str) {
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
                }
                {
                    if info_fields.get(ROLE).unwrap() == MASTER {
                        //broadcast_commands(all_lines.as_slice(), &broadcast_info);
                        broadcast_info.lock().unwrap().broadcast_command(&all_lines);
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
        }
    }
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
        // let Some(all_lines) = utils::decode_bulk_string(&stream) else {
        //     break;
        // };
        // if all_lines.len() <= 1 {
        //     eprintln!("COMMAND TOO SHORT: LINES {:?}", all_lines);
        //     stream.write_all(RESP_NULL).unwrap();
        //     continue;
        // }
        // eprintln!("ALL LINES:{:?}", all_lines);

        // let cmd = &all_lines[1];

        let mut conn = RespConnection::new(stream.try_clone().unwrap());
        match conn.try_read_command()? {
            Some(all_lines) => {
                match all_lines[1].to_lowercase().as_str() {
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
                                //broadcast_commands(all_lines.as_slice(), &broadcast_info);
                                broadcast_info.lock().unwrap().broadcast_command(&all_lines);
                            }
                            let k = all_lines[3].clone();
                            let v = all_lines[5].clone();

                            if all_lines.len() > 6 {
                                let r =
                                    handle_set(k, v, new_db, Some((&all_lines[7], &all_lines[9])));
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
                        config_response(config_command, config_field, stream, dir, db_filename);
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
                                    &[b"*", ret_keys.len().to_string().as_bytes(), b"\r\n"]
                                        .concat(),
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
                            lk.connections
                                .push(RespConnection::new(stream.try_clone().unwrap()));
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
                            create_dummy_rdb(&dummy_rdb_path.as_path())
                                .expect("FAILED TO MAKE DUMMY RDB");
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
            None => {
                break;
            }
        }
    }
    Ok(())
}
