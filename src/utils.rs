use crate::RdbFile;
use rand::Rng;
use std::io::{prelude::*, BufReader};
use std::net::TcpStream;

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
    [
        b"*3\r\n",
        get_bulk_string(first).as_slice(),
        get_bulk_string(second).as_slice(),
        get_bulk_string(third).as_slice(),
    ]
    .concat()
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
    let mut my_iter = BufReader::new(stream).lines();

    /*
     * if next returns None then no more lines
     */
    let arr_length = my_iter.next()?;

    /*
    * for each element we'll have 2 lines, one with the size and the other with the text
        so arr_length will ne provided num of elements * 2
    */
    let arr_length = &arr_length.expect("failed to unwrap arr length line from buf")[1..]
        .parse::<usize>()
        .expect("failed to get bulk string element num from stream");

    let n = arr_length * 2;
    for _ in 0..n {
        all_lines.push(my_iter.next()?.unwrap());
    }
    Some(all_lines)
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
