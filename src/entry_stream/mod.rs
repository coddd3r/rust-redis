use std::{
    collections::HashMap,
    io::Write,
    net::TcpStream,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::entry_stream::entry_utils::get_xread_resp_array;
use crate::utils::get_bulk_string;

pub mod entry_utils;
use crate::constants::*;

#[derive(Debug, Clone)]
pub struct RedisEntry {
    pub values: Vec<(String, String)>,
    pub next_sequence_id: Option<String>,
    pub prev_entry_id: Option<String>,
    pub insertion_time: SystemTime,
}

impl RedisEntry {
    pub fn new(v: Vec<(String, String)>, prev_entry_id: Option<String>) -> Self {
        Self {
            values: v,
            next_sequence_id: None,
            prev_entry_id,
            insertion_time: SystemTime::now(),
        }
    }

    pub fn entry_resp_array(&self) -> String {
        let mut use_vec: Vec<&str> = Vec::new();
        self.values.iter().for_each(|e| {
            use_vec.extend([&e.0.as_str(), &e.1.as_str()]);
        });

        let mut resp = format!("*{}\r\n", use_vec.len());
        for element in use_vec {
            resp.push_str(&format!("${}\r\n{}\r\n", element.len(), element));
        }
        resp
    }
}

#[derive(Debug, Default)]
pub struct RedisEntryStream {
    pub entries: HashMap<String, RedisEntry>,
    pub last_id: (usize, usize),
    pub sequences: HashMap<usize, usize>,
    pub first_sequence_id: Option<String>,
    pub last_sequence_id: Option<String>,
    //TODO: hashmap or vec?
    pub waiting_streams: HashMap<String, TcpStream>,
}

impl RedisEntryStream {
    pub fn new() -> Self {
        RedisEntryStream::default()
    }

    pub fn get_next_sequence(&mut self, seq: usize) -> usize {
        //eprintln!("in get sequence, SEQUENCES:{:?}", self.sequences);
        let ret = self.sequences.entry(seq).or_insert(0).clone();
        *(self.sequences.get_mut(&seq).unwrap()) += 1;
        //eprintln!("in get sequence, returning:{ret}");
        ret
    }

    //pub fn handle_add(&mut self, entry_id: &str, use_vec: Vec<(String, String)>) -> Vec<u8> {
    pub fn handle_add(&mut self, entry_id: &str, use_vec: Vec<(String, String)>) -> String {
        let res = self.stream_id_response(&entry_id);
        ////eprintln!("xadd result:{:?}", String::from_utf8_lossy(&res.1));
        //eprintln!("xadd result:{:?}", &res.1);

        if res.0 {
            let prev_sequence_id = &self.last_sequence_id;
            let use_entry = RedisEntry::new(use_vec, prev_sequence_id.clone());
            //let use_entry_id = String::from_utf8(res.1.clone()).unwrap();
            match prev_sequence_id {
                Some(p_id) => {
                    //eprintln!("USING EXISTING SEQ ID");
                    let prev_entry = self.entries.get_mut(p_id).unwrap();
                    prev_entry.next_sequence_id = Some(entry_id.to_string());
                    self.last_sequence_id = Some(entry_id.to_string());
                    //eprintln!("set prev entry:{:?}", prev_entry);
                }
                None => {
                    //eprintln!("ADDING NEW SEQ ID");
                    self.first_sequence_id = Some(entry_id.to_string());
                    self.last_sequence_id = Some(entry_id.to_string());
                }
            }

            //eprintln!("IN XADD waiting:{:?}", self.waiting_streams);
            if !self.waiting_streams.is_empty() {
                //eprintln!("\n WAITIN FORRR \n\n");
                for (k, mut st) in &self.waiting_streams {
                    let mut resp_args = Vec::new();
                    let mut stream_id_and_entry = Vec::new();
                    stream_id_and_entry.push((entry_id.to_string(), use_entry.clone()));
                    resp_args.push((k.clone(), stream_id_and_entry));
                    st.write_all(&get_xread_resp_array(&resp_args).as_bytes())
                        .unwrap();
                }
                //eprintln!("AFTER FOR");
                self.waiting_streams = HashMap::new();
            }

            //eprintln!("creating entry with vec:{:?}", use_entry);
            self.entries.insert(entry_id.to_string(), use_entry);

            //eprintln!("succesful insert curr stream:{:?}", self);
        }
        res.1
    }

    //pub fn stream_id_response(&mut self, id: &str) -> (bool, Vec<u8>) {
    pub fn stream_id_response(&mut self, id: &str) -> (bool, String) {
        let mut parts: Vec<_> = Vec::new();

        if id == "*" {
            let start = SystemTime::now();
            let since_the_epoch = start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as usize;
            self.last_id = (since_the_epoch, 0);
            let use_id = format!("{since_the_epoch}-0");
            self.sequences.insert(since_the_epoch, 1);
            //eprintln!("after inser:{:?}", self.sequences);
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
                        //return (false, SMALLER_ERROR.into());
                        return (false, SMALLER_ERROR.to_string());
                    }
                }
            }
        }
        //eprintln!("in xadd response, using parts:{:?}", parts);

        if parts[1] < 1 && parts[0] < 1 {
            //eprintln!("got 0");
            return (false, ZERO_ERROR.into());
        }

        if parts[0] > self.last_id.0 || (parts[0] == self.last_id.0 && parts[1] > self.last_id.1) {
            //eprintln!("parts[0] GREATER?");
            self.last_id = (parts[0], parts[1]);
            let use_id = format!("{}-{}", parts[0], parts[1]);
            //eprintln!("returning id:{use_id}");
            return (true, get_bulk_string(&use_id));
        }

        return (false, SMALLER_ERROR.into());
    }

    //pub fn get_from_range(&self, start: &str, end: &str) -> Vec<u8> {
    pub fn get_from_range(&self, start: &str, end: &str) -> String {
        //eprintln!("IN XRANGE FUNC, curr entries:{:?}", self.entries);
        let mut check_keys = Vec::new();
        if self.first_sequence_id.is_none() {
            return RESP_NULL.into();
        }

        let start_time = &{
            if start.chars().nth(0).unwrap() == '-' {
                //eprintln!("START IS \"-\"");
                self.first_sequence_id.clone().unwrap()
            } else if start.contains('-') {
                start.to_string()
            } else {
                format!("{start}-{}", 0)
            }
        };

        let end_id = {
            if end == "+" {
                self.last_sequence_id.as_ref().unwrap().as_str()
            } else {
                end
            }
        };

        //eprintln!("using start:{start_time}, end:{end_id}");
        match self.entries.get(start_time) {
            Some(_ent) => {
                //eprintln!("got a start entry:{:?}", ent);
                let mut curr_id = Some(start_time);
                loop {
                    //eprintln!("in range-loop");
                    match curr_id {
                        Some(use_id) => {
                            //eprintln!("found next wntry using id:{use_id}");
                            if !end_id.contains('-') && use_id.split('-').nth(0).unwrap() > end_id {
                                //eprintln!("BREAKING in first loop");
                                break;
                            } else if end_id.contains('-') && use_id.as_str() > end_id {
                                //eprintln!("GOT TO END of range breaking with:{:?}", check_keys);
                                break;
                            }

                            let curr = self.entries.get(curr_id.unwrap()).unwrap();
                            check_keys.push((use_id.clone(), curr.clone()));
                            curr_id = curr.next_sequence_id.as_ref();
                        }
                        None => {
                            //eprintln!("next id is none, breaking with:{:?}", check_keys);
                            break;
                        }
                    }
                }
            }
            None => return self.get_stream_resp_array(&check_keys),
        }

        //eprintln!("Got check keys:{:?}", check_keys);
        let resp_arrays = self.get_stream_resp_array(&check_keys);
        //eprintln!("resp arrays as resp:{:?}", resp_arrays);
        resp_arrays
    }

    pub fn get_stream_resp_array(&self, v: &Vec<(String, RedisEntry)>) -> String {
        if v.is_empty() {
            //eprintln!("getting resp arr for empty");
            return RESP_NULL.into();
        }

        let mut resp = format!("*{}\r\n", v.len());
        v.iter().for_each(|(entry_id, ent)| {
            resp.push_str("*2\r\n");
            resp.push_str(&get_bulk_string(&entry_id));
            resp.push_str(&ent.entry_resp_array());
        });

        resp
    }
    //pub fn xread_range(&self, stream_name: &str, start: &str) -> Vec<u8> {
    pub fn xread_range(
        &self,
        stream_name: &str,
        start: &str,
    ) -> Option<(String, Vec<(String, RedisEntry)>)> {
        //eprintln!("IN XREAD FUNC, curr entries:{:?}", self.entries);
        let mut check_keys = Vec::new();
        let start_t = {
            if self.first_sequence_id.is_none() {
                //eprintln!("empty stream");
                None
            } else if self.first_sequence_id.is_some() {
                if self
                    .first_sequence_id
                    .clone()
                    .unwrap()
                    .split('-')
                    .into_iter()
                    .gt(start.split('-').into_iter())
                {
                    self.first_sequence_id.clone()
                } else {
                    None
                }
            } else if *start < *self.first_sequence_id.clone().unwrap() {
                self.first_sequence_id.clone()
            } else if start == "-" {
                Some(self.first_sequence_id.clone().unwrap())
            } else if start.contains('-') {
                let excl_id = start.to_string();

                match self.entries.get(&excl_id) {
                    Some(ent) => Some(ent.next_sequence_id.clone().unwrap()),
                    None => None,
                }
            } else {
                Some(format!("{start}-{}", 0))
            }
        };

        let start_id;
        if start_t.is_some() {
            start_id = start_t.clone().unwrap();
        } else {
            return None;
        }

        //eprintln!("using start:{start_id}");
        match self.entries.get(&start_id) {
            Some(_ent) => {
                //eprintln!("got a start entry:{:?}", ent);
                let mut curr_id = Some(&start_id);
                loop {
                    //eprintln!("in range-loop");
                    match curr_id {
                        Some(use_id) => {
                            //eprintln!("found next entry using id:{use_id}");
                            let curr = self.entries.get(use_id).unwrap();
                            check_keys.push((use_id.clone(), curr.clone()));
                            curr_id = curr.next_sequence_id.as_ref();
                        }
                        None => {
                            //eprintln!("next id is none, breaking with:{:?}", check_keys);
                            break;
                        }
                    }
                }
            }
            None => {}
        }

        //eprintln!("Got check keys:{:?}", check_keys);

        Some((stream_name.to_string(), check_keys))
    }

    pub fn block_xread(
        &self,
        stream_name: &str,
        block_start_time: SystemTime,
        time_to_block_for: Duration,
        start: &str,
    ) -> Option<(String, Vec<(String, RedisEntry)>)> {
        //eprintln!(
        //    "IN XBLOCK FUNC checking for entries starting:{:?}\n, until {:?}\n, blocking for {:?}\n, curr entries:{:?}\n, stream_name{stream_name},\n start seqid:{start}",
        //    block_start_time,
        //    block_start_time + time_to_block_for,
        //    time_to_block_for,
        //    self.entries
        //);
        let mut check_keys = Vec::new();
        if self.first_sequence_id.is_none() && self.last_sequence_id.is_none() {
            return None;
        }
        // the last id to use checking for items added during block
        // default last sequence id used
        let block_start = {
            if self.last_sequence_id.is_none() && self.first_sequence_id.is_some() {
                &self.first_sequence_id
            } else {
                &self.last_sequence_id
            }
        };

        let start_t = {
            if self.first_sequence_id.is_none() {
                //eprintln!("empty stream");
                //eprintln!("No fierst seq id");
                None
            } else if self.first_sequence_id.is_some() {
                if self
                    .first_sequence_id
                    .clone()
                    .unwrap()
                    .split('-')
                    .into_iter()
                    .ge(start.split('-').into_iter())
                {
                    self.first_sequence_id.clone()
                } else {
                    //eprintln!("in coparison returning start time none");
                    None
                }
            } else if *start < *self.first_sequence_id.clone().unwrap() {
                self.first_sequence_id.clone()
            } else if start == "-" {
                Some(self.first_sequence_id.clone().unwrap())
            } else if start.contains('-') {
                let excl_id = start.to_string();

                match self.entries.get(&excl_id) {
                    Some(ent) => Some(ent.next_sequence_id.clone().unwrap()),
                    None => {
                        //eprintln!("in exclusive returning start None");
                        None
                    }
                }
            } else {
                Some(format!("{start}-{}", 0))
            }
        };

        let start_id;
        if start_t.is_some() {
            start_id = start_t.clone().unwrap();
        } else {
            //eprintln!("breaking starttime none");
            return None;
        }

        match self.entries.get(block_start.as_ref().unwrap()) {
            Some(_ent) => {
                //eprintln!("got a start entry:{:?}", ent);
                let mut curr_id = block_start;
                loop {
                    //eprintln!("in range-loop");
                    match curr_id {
                        Some(use_id) => {
                            //eprintln!("found prev entry using id:{use_id}");
                            if use_id < &start_id {
                                //eprintln!("BREAKING XBLOCK went below seq start");
                                break;
                            }
                            let curr = self.entries.get(use_id).unwrap();
                            if curr.insertion_time < block_start_time
                                || curr.insertion_time > block_start_time + time_to_block_for
                            {
                                //eprintln!("BLOCK EXCEEDED time, breaking\n\n");
                                break;
                            }
                            check_keys.push((use_id.clone(), curr.clone()));
                            curr_id = &curr.prev_entry_id;
                        }
                        None => {
                            //eprintln!("PREVIOUS id is none, breaking with:{:?}", check_keys);
                            break;
                        }
                    }
                }
            }
            None => {}
        }

        //eprintln!("Got check keys:{:?}", check_keys);
        if !check_keys.is_empty() {
            Some((stream_name.to_string(), check_keys))
        } else {
            None
        }
    }
}
