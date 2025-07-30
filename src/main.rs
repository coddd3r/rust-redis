use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex};

use codecrafters_redis::entry_stream::RedisEntryStream;
use codecrafters_redis::handler::handle_connection;
use codecrafters_redis::redis_connection::broadcast_info::BroadCastInfo;
use codecrafters_redis::redis_database::{read_rdb_file, RedisDatabase};

use codecrafters_redis::redis_subscriber::{Channel, Subscriber};
use codecrafters_redis::threadpool::ThreadPool;

use codecrafters_redis::redis_list::RedisList;
use codecrafters_redis::utils::random_id_gen;

use codecrafters_redis::constants::*;

fn main() {
    let id = random_id_gen();
    let mut info_fields: HashMap<String, String> = HashMap::new();
    info_fields.insert(String::from(ROLE), MASTER.to_string());
    //eprintln!("ID:{:?}", id);
    //info_fields.insert("id", id);

    let arg_list = std::env::args();
    let mut dir = None;
    let mut db_filename = None;
    let mut full_port = String::from("127.0.0.1:");
    let mut port_found = false;
    let mut short_port = String::new();

    let stream_pool = ThreadPool::new(25);
    let mut master_port: Option<String> = None;

    let broadcast_info: Arc<Mutex<BroadCastInfo>> = Arc::new(Mutex::new(BroadCastInfo::new()));
    let mut new_db = Arc::new(Mutex::new(RedisDatabase::new()));

    let streams_db: HashMap<String, RedisEntryStream> = HashMap::new();
    let streams_db = Arc::new(Mutex::new(streams_db));

    let channels_db: HashMap<String, Channel> = HashMap::new();
    let channels_db = Arc::new(Mutex::new(channels_db));

    let lists_map: HashMap<String, RedisList> = HashMap::new();
    let lists_map = Arc::new(Mutex::new(lists_map));

    let subscribers_db: HashMap<String, Subscriber> = HashMap::new();
    let subscribers_db = Arc::new(Mutex::new(subscribers_db));

    let mut b = arg_list.into_iter();
    while let Some(a) = b.next() {
        match a.as_str() {
            "--dir" => {
                dir = b.next();
            }
            "--dbfilename" => {
                db_filename = b.next();
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
                                let st_db = Arc::clone(&streams_db);
                                let list_map = Arc::clone(&lists_map);
                                let channel_db = Arc::clone(&channels_db);
                                let subscriber_db = Arc::clone(&subscribers_db);
                                stream_pool.execute(move || {
                                    let res = handle_connection(
                                        use_stream,
                                        None,
                                        None,
                                        i_fields,
                                        b_info,
                                        &Some(short_port.as_str()),
                                        &m_port,
                                        use_db,
                                        st_db,
                                        list_map,
                                        channel_db,
                                        subscriber_db,
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
                eprintln!("\n\naccepted new connection\n\n");
                let dir_arg = dir.clone();
                let db_arg = db_filename.clone();
                let i_fields = info_fields.clone();
                let m_port = master_port.clone();

                let b_info = Arc::clone(&broadcast_info);
                let use_db = Arc::clone(&new_db);
                //let s = Arc::new(Mutex::new(_stream));
                let s = _stream.try_clone().unwrap();

                let short_port = short_port.clone();
                let st_db = Arc::clone(&streams_db);
                let list_map = Arc::clone(&lists_map);
                let channel_db = Arc::clone(&channels_db);
                let subscriber_db = Arc::clone(&subscribers_db);
                stream_pool.execute(move || {
                    let res = handle_connection(
                        s,
                        dir_arg,
                        db_arg,
                        i_fields,
                        b_info,
                        &Some(short_port.as_str()),
                        &m_port,
                        use_db,
                        st_db,
                        list_map,
                        channel_db,
                        subscriber_db,
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
