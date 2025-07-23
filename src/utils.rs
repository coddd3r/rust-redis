use crate::RdbFile;
use rand::Rng;
use std::io::{prelude::*, BufReader};
use std::net::TcpStream;

use std::fs::File;
use std::io::{BufWriter, Read, Write};

pub fn get_bulk_string(res: &str) -> Vec<u8> {
    //fn get_bulk_string(res: &str) -> &[u8] {
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

pub fn get_repl_bytes(first: &str, second: &str, third: &str) -> Vec<u8> {
    write_resp_arr(vec![first, second, third])
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
pub fn decode_bulk_string(stream: &TcpStream) -> Option<Vec<String>> {
    let mut all_lines = Vec::new();
    let mut bulk_reader = BufReader::new(stream);
    let mut first_line = String::new();
    bulk_reader.read_line(&mut first_line).unwrap();
    if first_line.is_empty() {
        eprintln!("EMPTY LINE");
        return None;
    }
    if first_line.chars().nth(0).unwrap() != '*' {
        eprintln!("FOUND LENGTH RESPONSE:{first_line}");

        for x in first_line.trim().chars() {
            eprintln!("digit?, {x}");
        }
        let rdb_len = first_line.trim()[1..]
            .parse::<usize>()
            .expect("failed to parse rdb length");
        let mut received_rdb: Vec<u8> = vec![0u8; rdb_len];
        eprintln!("writing to vec with capacity:{:?}", received_rdb.capacity());
        bulk_reader
            .read_exact(&mut received_rdb)
            //.read_until(0xFF, &mut received_rdb)
            .expect("FAILED TO READ RDB BYTES");

        //eprintln!("read from stream num bytes:{num_bytes_read}");
        eprintln!(
            "read from stream num rdb file:{:?}, length:{:?}",
            received_rdb,
            received_rdb.len()
        );

        let received_rdb_path = std::env::current_dir().unwrap().join("dumpreceived.rdb");

        let mut file = File::create(&received_rdb_path).unwrap();
        file.write_all(&received_rdb)
            .expect("failed to write receive rdb to file");
        eprintln!("WRPTE RESPONSE TO FILE");
        let rdb = codecrafters_redis::read_rdb_file(received_rdb_path)
            .expect("failed tp read response rdb from file");
        eprintln!("RDB:{:?}", rdb);
        eprintln!("final all lines{:?}", all_lines);
    } else {
        eprintln!("initial array length{first_line}");
        let mut my_iter = bulk_reader.lines().peekable();

        // for each element we'll have 2 lines, one with the size and the other with the text
        //   so arr_length will ne provided num of elements * 2
        let arr_length = first_line.trim()[1..]
            .parse::<usize>()
            .expect("failed to get bulk string element num from stream");

        let n = arr_length * 2;
        eprintln!("GOT SIZE:{n}");

        for _ in 0..n {
            all_lines.push(my_iter.next()?.unwrap());
        }
    }
    Some(all_lines)
}

pub fn read_response(st: &TcpStream, n: Option<usize>) -> String {
    let mut buf_reader = BufReader::new(st.try_clone().unwrap());
    let mut use_buf = String::new();
    let _ = buf_reader.read_line(&mut use_buf);

    if let Some(x) = n {
        eprintln!("{x}th handshake done, response:{}", use_buf);
    }
    use_buf
}

pub fn write_resp_arr(cmd: Vec<&str>) -> Vec<u8> {
    let mut full_bytes = Vec::new();
    full_bytes.extend_from_slice(b"*");
    full_bytes.extend_from_slice(cmd.len().to_string().as_bytes());
    full_bytes.extend_from_slice(b"\r\n");

    cmd.iter()
        .for_each(|e| full_bytes.extend(get_bulk_string(e)));
    full_bytes
}

//fn handle_set(db: RedisDatabase, key: String, val: RedisValue, exp: Option<Expiration>) {}
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
