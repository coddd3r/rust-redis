use rand::Rng;
use std::error::Error;
use std::io::{prelude::*, BufReader};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::time::UNIX_EPOCH;

use codecrafters_redis::print_hex::print_hex_dump;
use codecrafters_redis::{
    print_hex, read_rdb_file, write_rdb_file, Expiration, RdbError, RdbFile, RedisDatabase,
    RedisValue,
};

use crate::resp_parser::{BroadCastInfo, RespConnection};

//pub fn get_bulk_string(res: &str) -> Vec<u8> {
pub fn get_bulk_string(res: &str) -> String {
    //fn get_bulk_string(res: &str) -> &[u8] {
    let res_size = res.len();
    // [
    //     b"$",
    //     res_size.to_string().as_bytes(),
    //     b"\r\n",
    //     res.as_bytes(),
    //     b"\r\n",
    // ]
    // .concat()

    format!("${res_size}\r\n{res}\r\n")
}

//pub fn get_redis_int(n: i32) -> Vec<u8> {
pub fn get_redis_int(n: i32) -> String {
    //format!(":{n}\r\n").as_bytes().into()
    format!(":{n}\r\n")
}
pub fn random_id_gen() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                            abcdefghijklmnopqrstuvwxyz\
                            0123456789";
    const ID_LEN: usize = 40;
    let mut rng = rand::rng();

    let id: String = (0..ID_LEN)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();
    id
}

pub fn read_rdb_keys(rdb: RdbFile, search_key: String) -> Vec<String> {
    ////eprintln!("Successful rdb read");
    let mut ret_keys = Vec::new();
    //get by index
    // TODO! instead of hardcoding, find the latest key, i.e largest num
    if let Some(db) = rdb.databases.get(&0) {
        ////eprintln!("GOT DB ROM RDB FILE {:?}", db);
        match search_key.as_str() {
            "*" => {
                ////eprintln!("GOT * search");
                db.data.clone().into_iter().for_each(|(k, _)| {
                    ret_keys.push(k);
                });
            }
            _others => {
                let search_strings: Vec<&str> = search_key.split("*").collect();
                db.data.clone().into_iter().for_each(|(k, _)| {
                    if search_strings.iter().all(|e| k.contains(e)) {
                        ret_keys.push(k);
                    }
                });
            }
        }
    }
    ////eprintln!("All KEYS to return:{:?}", ret_keys);
    ret_keys
}

/**
*
*   https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings
    /The exact bytes your program will receive won't be just ECHO hey, you'll receive something like this: *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n. That's ["ECHO", "hey"] encoded using the Redis protocol.
*
**/
//pub fn decode_bulk_string(stream: &TcpStream) -> Option<Vec<String>> {
//    let mut all_lines = Vec::new();
//    let mut bulk_reader = BufReader::new(stream.try_clone().unwrap());
//    let mut first_line = String::new();
//    bulk_reader.read_line(&mut first_line).unwrap();
//    if first_line.is_empty() {
//        //eprintln!("EMPTY LINE");
//        return None;
//    }
//    //eprintln!("first line NOT empty, {first_line}");
//    let first_char = first_line.chars().nth(0).unwrap();
//    match first_char {
//        '*' => {
//            //eprintln!("initial array length{first_line}");
//            let mut my_iter = bulk_reader.lines().peekable();
//
//            // for each element we'll have 2 lines, one with the size and the other with the text
//            //   so arr_length will ne provided num of elements * 2
//            let arr_length = first_line.trim()[1..]
//                .parse::<usize>()
//                .expect("failed to get bulk string element num from stream");
//
//            let n = arr_length * 2;
//            //eprintln!("GOT SIZE:{n}");
//
//            for _ in 0..n {
//                all_lines.push(my_iter.next()?.unwrap());
//            }
//        }
//        '$' => {
//            if ['+', '-', ':']
//                .iter()
//                .any(|e| e == &first_line.chars().nth(1).unwrap())
//            {
//                //eprintln!("DECODING BULK, IGNORING: {first_line}");
//            } else {
//                //eprintln!("DECODING BULK, READING RDB: {first_line}");
//                let rdb_len = first_line[1..]
//                    .trim()
//                    .parse::<usize>()
//                    .expect("failed to parse rdb length");
//
//                //let rdb_bytes = read_db_from_stream(rdb_len, bulk_reader);
//                //decode_rdb(rdb_bytes);
//                //eprintln!("IGNORING RDB IN BULK READER");
//                bulk_reader.consume(rdb_len);
//            }
//        }
//        '+' | '-' | ':' => {
//            //eprintln!("DECODING BULK, IGNORING: {first_line}");
//        }
//        _ => {
//            //eprintln!("\r\nINVALID START OF COMMAND\r\n");
//        }
//    }
//    Some(all_lines)
//}

//pub fn read_response(st: &TcpStream, n: Option<usize>) -> String {
//    ////eprintln!("reading response from:{:?}", st);
//    let mut buf_reader = BufReader::new(st.try_clone().unwrap());
//    let mut use_buf = String::new();
//    ////eprintln!("in read_response before read line:{:?}", buf_reader);
//    let _ = buf_reader.read_line(&mut use_buf);
//
//    if let Some(r) = n {
//        eprintln!("{r}th");
//    }
//    eprintln!(" finished reading response from stream: {use_buf}");
//    use_buf
//}

//pub fn write_resp_arr(elements: &[&str]) -> Vec<u8> {
//    let mut resp = format!("*{}\r\n", elements.len()).into_bytes();
//    for element in elements {
//        resp.extend(format!("${}\r\n{}\r\n", element.len(), element).into_bytes());
//    }
//    resp
//}
pub fn get_port(stream: &TcpStream) -> Option<String> {
    if let Ok(peer_addr) = stream.peer_addr() {
        println!("Accepted connection from: {}", peer_addr);
        Some(peer_addr.port().to_string())
    } else {
        println!("Unable to get peer address.");
        None
    }
}

//pub fn config_response(
//    config_command: String,
//    config_field: String,
//    conn: RespConnection,
//    dir: Option<String>,
//    db_filename: Option<String>,
//) -> Result<(), Box<dyn Error>> {
//    match config_command.as_str() {
//        "get" => match config_field.as_str() {
//            "dir" => {
//                if let Some(dir_name) = &dir {
//                    conn.write_to_stream(&conn.format_resp_array(&[&config_field, dir_name]));
//                    //stream.write_all(&resp).unwrap();
//                } else {
//                    //stream.write_all(crate::RESP_NULL)?;
//                    conn.write_to_stream(crate::RESP_NULL);
//                }
//            }
//            "dbfilename" => {
//                if let Some(db_name) = &db_filename {
//                    conn.write_to_stream(&conn.format_resp_array(&[&config_field, &db_name]));
//                } else {
//                    conn.write_to_stream(crate::RESP_NULL);
//                }
//            }
//            _ => {
//                eprintln!("UNRECOGNIZED GET CONFIG FIELD");
//            }
//        },
//        _ => {
//            eprintln!("UNRECOGNIZED CONFIG COMMAND")
//        }
//    }
//    Ok(())
//}

//pub fn broadcast_commands(cmd: &[String], b_info: &Arc<Mutex<BroadCastInfo>>) {
//    //eprintln!("in BROADCAST, info:{:?}", b_info);
//
//    let broadcast_bytes = write_resp_arr(
//        cmd.iter()
//            .filter(|e| !e.starts_with('$'))
//            .map(|e| e.as_str())
//            .collect::<Vec<_>>(),
//    );
//
//    let (conn, client_ports) = {
//        let curr_info = b_info.lock().unwrap();
//        (curr_info.connections.clone(), curr_info.ports.clone())
//    };
//
//    for (i, conn) in conn.iter().enumerate() {
//        let mut c = conn.stream.lock().unwrap();
//        //eprintln!(
//        //     "in client streams, port:{}, stream:{:?}",
//        //     client_ports[i], c
//        // );
//        //eprintln!(
//        //     "broadcast MESSAGE: {:?}",
//        //     String::from_utf8(broadcast_bytes.clone()).unwrap()
//        // );
//        c.write_all(&broadcast_bytes)
//            .expect("FAILED TO PING master");
//        //eprintln!("wrote broadcst to port");
//    }
//    //eprintln!("after clients lopp in broadcast");
//}

pub fn handle_set(
    k: String,
    v: String,
    new_db: &Arc<Mutex<RedisDatabase>>,
    expiry_info: Option<(&str, &str)>,
) -> Result<(), Box<RdbError>> {
    eprintln!("HANDLING SET FOR K:{k}, V:{v}");
    let mut use_insert = RedisValue {
        value: v,
        expires_at: None,
    };
    if let Some((expiry_type, expiry_time)) = expiry_info {
        match expiry_type {
            "px" => {
                let time_arg: u64 = expiry_time.parse().expect("failed to parse expiry time");
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64; ////eprintln!("got MILLISECONDS expiry:{time_arg}");
                let end_time_s = now + time_arg;
                ////eprintln!("AT: {now}, MSexpiry:{time_arg},end:{end_time_s}");
                let use_expiry = Some(Expiration::Milliseconds(end_time_s));
                use_insert.expires_at = use_expiry;
            }
            "ex" => {
                let time_arg: u32 = expiry_time.parse().expect("failed to parse expiry time");
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let end_time_s = now as u32 + time_arg;

                ////eprintln!("AT: {now}, got SECONDS expiry:{time_arg}, expected end:{end_time_s}");
                let use_expiry = Some(Expiration::Seconds(end_time_s as u32));
                use_insert.expires_at = use_expiry;
            }
            _ => {
                return Err(Box::new(RdbError::UnsupportedFeature(
                    "WRONG SET ARGUMENTS",
                )))
            }
        }
        ////eprintln!("before inserting in db, expiry:{:?}", use_expiry);
    }

    {
        eprintln!("IN HANDLE SET FUNCTION, BEFORE LOCK");
        let mut lk = new_db.lock().unwrap();
        lk.insert(k.clone(), use_insert);
        let res = lk.get(&k);
        eprintln!("IN HANDLE SET FUNCTION, AFTER LOCK GET RES: {:?}", res);
    }
    Ok(())
}

//pub fn handle_get(
//    get_key: &str,
//    //stream: &mut TcpStream,
//    conn: RespConnection,
//    new_db: &Arc<Mutex<RedisDatabase>>,
//) -> Result<(), RdbError> {
//    {
//        //eprintln!("in handle GET function before lock");
//        let mut lk = new_db.lock().expect("failed to lock db in get");
//        //eprintln!("in handle GET function locked db:{:?}", lk);
//        if let Some(res) = lk.get(&get_key) {
//            if res.expires_at.is_some() && res.expires_at.as_ref().unwrap().is_expired() {
//                //eprintln!("ASKING FOR EXPIRED!!?? key: {get_key}");
//                lk.data.remove(get_key);
//                conn.write_to_stream(crate::RESP_NULL);
//            } else {
//                let resp = crate::utils::get_bulk_string(&res.value);
//                conn.write_to_stream(&resp);
//            }
//        } else {
//            //eprintln!("IN GET FOUND NONE");
//            conn.write_to_stream(crate::RESP_NULL);
//        }
//    }
//    Ok(())
//}
//
//pub fn read_rdb_from_stream<R: Read>(rdb_len: usize, mut bulk_reader: R) -> Vec<u8> {
//    ////eprintln!("IN FUNCTION GO STREAM SIZE: {first_line}");
//    let mut received_rdb: Vec<u8> = vec![0u8; rdb_len];
//    //eprintln!("writing to vec with capacity:{:?}", received_rdb.capacity());
//    bulk_reader
//        .read_exact(&mut received_rdb)
//        //.read_until(0xFF, &mut received_rdb)
//        .expect("FAILED TO READ RDB BYTES");
//
//    received_rdb
//    ////eprintln!("read from stream num bytes:{num_bytes_read}");
//}
//
//pub fn decode_rdb(received_rdb: Vec<u8>) {
//    //eprintln!("DECODING RDB BYTES RECEIVED");
//    //eprintln!(
//        "read from stream num rdb file:{:?}, length:{:?}",
//        received_rdb,
//        received_rdb.len()
//    );
//    print_hex_dump(&received_rdb);
//
//    let received_rdb_path = std::env::current_dir().unwrap().join("dumpreceived.rdb");
//
//    let mut file = File::create(&received_rdb_path).unwrap();
//    file.write_all(&received_rdb)
//        .expect("failed to write receive rdb to file");
//    //eprintln!("WRPTE RESPONSE TO FILE");
//    let final_rdb = codecrafters_redis::read_rdb_file(received_rdb_path)
//        .expect("failed tp read response rdb from file");
//    //eprintln!("RECEIVED RDB:{:?}", final_rdb);
//}

//fn get_simple_string(s: &str) -> Vec<u8> {
//    [b"+", s.as_bytes(), b"\r\n"].concat()
//}
//fn get_simple_string_vec(v: Vec<&str>) -> Vec<&[u8]> {
//    let input_strings: Vec<&[u8]> = v.iter().map(|e| e.as_bytes()).collect();
//    let mut x: Vec<&[u8]> = Vec::new();
//    x.push("+".as_bytes());
//    x.extend(input_strings);
//    x.push("\r\n".as_bytes());
//    x
//}
// fn parse_buffer(&mut self) -> std::io::Result<Option<Vec<Vec<String>>>> {
//     eprintln!("PARSING BUFFER, starting at pos:{}", self.position);
//     eprintln!("WHOLE BUFFER:{:?}", String::from_utf8_lossy(&self.buffer));
//     //let mut lines = self.buffer[self.position..].split(|&b| b == b'\n');
//     if let Ok(parsed_string) = String::from_utf8(self.buffer[self.position..].into()) {
//         let mut lines = parsed_string.split("\r\n");
//         let mut commands = Vec::new();

//         while let Some(line_str) = lines.next() {
//             if line_str.is_empty() {
//                 //eprintln!("EMPTY  BREAKING");
//                 //break;
//                 eprintln!("EMPTY CONTINUE");
//                 continue;
//             }

//             match line_str.chars().next() {
//                 Some('*') => {
//                     // Array type
//                     let arr_length = match line_str[1..].trim().parse::<usize>() {
//                         Ok(n) => n,
//                         Err(_) => continue,
//                     };
//                     eprintln!(
//                         "adding to pos ofr arr length line in resp arr before: {}",
//                         self.position
//                     );
//                     self.position += line_str.len() + 2; // +2 for \r\n since we split at CRLF
//                     eprintln!("after:{}", self.position);

//                     let mut elements = Vec::with_capacity(arr_length);
//                     let mut valid = true;

//                     for _ in 0..arr_length {
//                         eprintln!(
//                             "adding to pos afte line in resp arr before: {}",
//                             self.position
//                         );
//                         self.position += line_str.len() + 2; // +1 for newline
//                         eprintln!("after:{}", self.position);

//                         let size_line = lines.next().unwrap();
//                         if !size_line.starts_with('$') {
//                             valid = false;
//                             break;
//                         }

//                         // Get bulk string content
//                         let size = match size_line[1..].trim().parse::<usize>() {
//                             Ok(n) => n,
//                             Err(_) => {
//                                 valid = false;
//                                 break;
//                             }
//                         };

//                         if let Some(content) = lines.next() {
//                             if content.len() != size {
//                                 valid = false;
//                                 break;
//                             }
//                             elements.push(content.to_string());
//                         };
//                         //elements.push(content);
//                     }

//                     if valid && elements.len() == arr_length {
//                         commands.push(elements);
//                     } else {
//                         eprintln!("valid?{valid}, elements?{:?}", elements);
//                     }
//                 }
//                 Some('$') => {
//                     // Bulk string (RDB file transfer)
//                     let rdb_len = match line_str[1..].trim().parse::<usize>() {
//                         Ok(n) => n,
//                         Err(_) => continue,
//                     };

//                     // Skip RDB data
//                     let rdb_start = self.position + line_str.len() + 1;
//                     let rdb_end = rdb_start + rdb_len + 2; // +2 for \r\n

//                     if self.buffer.len() >= rdb_end {
//                         eprintln!("SHOULDNT BE MOVING RDB END");
//                         self.position = rdb_end;
//                         eprintln!("AFTER MOVING ILLEGAL RDB END:{}", self.position);
//                     } else {
//                         break; // Wait for more data
//                     }
//                 }
//                 _ => continue, // Skip other RESP types
//             }
//         }

//         Ok(if !commands.is_empty() {
//             Some(commands)
//         } else {
//             None
//         })
//     } else {
//         eprintln!(
//             "need to parse RDB, curr pos:{}, got string:{:?}",
//             self.position,
//             String::from_utf8_lossy(&self.buffer[self.position..])
//         );
//         self.handle_rdb_transfer()
//     }
// }
//let re = r"$88\r\nREDIS0011\u{fa}\tredis-ver\x057.2.0\u{fa}\nredis-bits\u{c0}@\u{fa}\u{05}ctime\u{c2}m\b\u{bc}e\u{fa}\bused-mem°\u{c4}\x10\x00\u{fa}\baof-base\u{c0}\x00\u{ff}\u{f0}n;\u{fe}\u{c0}\u{ff}Z\u{a2}".as_bytes();
//                        let res = r"REDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\b\xbce\xfa\bused-mem°\xc4\x10\x00\xfa\baof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2".as_bytes();
