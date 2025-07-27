use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::utils::get_bulk_string;

/*
* XADD some_key 1526985054069-0 temperature 36 humidity 95
* */

const ZERO_ERROR: &[u8] = b"-ERR The ID specified in XADD must be greater than 0-0\r\n";
const SMALLER_ERROR: &[u8] =
    b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";

#[derive(Debug, Default, Clone)]
pub struct RedisEntry {
    pub values: Vec<(String, String)>,
    pub next_sequence_id: Option<String>,
}

impl RedisEntry {
    pub fn new(v: Vec<(String, String)>) -> Self {
        Self {
            values: v,
            next_sequence_id: None,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct RedisEntryStream {
    pub entries: HashMap<String, RedisEntry>,
    pub last_id: (usize, usize),
    pub sequences: HashMap<usize, usize>,
    pub last_sequence_id: Option<String>,
}

impl RedisEntryStream {
    pub fn new() -> Self {
        RedisEntryStream::default()
    }

    pub fn get_next_sequence(&mut self, seq: usize) -> usize {
        eprintln!("in get sequence, SEQUENCES:{:?}", self.sequences);
        let ret = self.sequences.entry(seq).or_insert(0).clone();
        *(self.sequences.get_mut(&seq).unwrap()) += 1;
        eprintln!("in get sequence, returning:{ret}");
        ret
    }

    pub fn stream_id_response(&mut self, id: &str) -> (bool, Vec<u8>) {
        let mut parts: Vec<_> = Vec::new();
        eprintln!("max usize:{}", usize::MAX);

        if id == "*" {
            let start = SystemTime::now();
            let since_the_epoch = start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as usize;
            self.last_id = (since_the_epoch, 0);
            let use_id = format!("{since_the_epoch}-0");
            self.sequences.insert(since_the_epoch, 1);
            eprintln!("after inser:{:?}", self.sequences);
            return (true, get_bulk_string(&use_id));
        }

        for part in id.split("-") {
            match part.parse::<usize>() {
                Ok(c) => parts.push(c),
                Err(_) => {
                    if part == "*" && !parts.is_empty() {
                        let got_seq = self.get_next_sequence(parts[0]);
                        let seq_num = {
                            if parts[0] == 0 && got_seq == 0 {
                                got_seq + 1
                            } else {
                                got_seq
                            }
                        };

                        parts.push(seq_num);
                    } else {
                        return (false, SMALLER_ERROR.into());
                    }
                }
            }
        }
        eprintln!("in xadd response, using parts:{:?}", parts);

        if parts[1] < 1 && parts[0] < 1 {
            eprintln!("got 0");
            return (false, ZERO_ERROR.into());
        }
        if parts[0] > self.last_id.0 || (parts[0] == self.last_id.0 && parts[1] > self.last_id.1) {
            eprintln!("parts[0] GREATER?");
            self.last_id = (parts[0], parts[1]);
            let use_id = format!("{}-{}", parts[0], parts[1]);
            eprintln!("returning id:{use_id}");
            return (true, get_bulk_string(&use_id));
        }
        return (false, SMALLER_ERROR.into());
    }

    pub fn get_from_range(&self, start: &str, end: &str) {
        eprintln!("IN XRANGE FUNC, curr entries:{:?}", self.entries);
        let mut check_keys = Vec::new();
        //let start_time = start.parse::<usize>().unwrap();
        let start_time = {
            if start.contains('-') {
                start.to_string()
            } else {
                format!("{start}-{}", 0)
            }
        };

        eprintln!("using start:{start_time}");
        match self.entries.get(&start_time) {
            Some(ent) => {
                eprintln!("got a start entry:{:?}", ent);
                let mut curr = ent;
                let mut curr_id = Some(&start_time);
                loop {
                    eprintln!("in range-loop");
                    match curr_id {
                        Some(use_id) => {
                            eprintln!("found next wntry using id:{use_id}");
                            if use_id.split("-").nth(0).unwrap() > end {
                                eprintln!("GOT TO END of range breaking with:{:?}", check_keys);
                                break;
                            }

                            curr = self.entries.get(curr_id.unwrap()).unwrap();
                            check_keys.push((use_id.clone(), curr.values.clone()));
                            curr_id = curr.next_sequence_id.as_ref();
                        }
                        None => {
                            eprintln!("next id is none, breaking with:{:?}", check_keys);
                            break;
                        }
                    }
                }
            }
            None => {
                panic!("NO ENTRY?")
            }
        }
    }
}
