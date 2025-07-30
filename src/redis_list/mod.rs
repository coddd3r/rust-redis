use std::io::Write;
use std::{net::TcpStream, time::SystemTime};

use crate::utils::get_resp_from_string;

#[derive(Debug)]
pub struct RedisList {
    pub name_key: String,
    pub values: Vec<String>,
    pub blocking_pop_streams: Vec<TcpStream>,
    pub blocking_until: SystemTime,
}

impl RedisList {
    pub fn new(name_key: String) -> Self {
        Self {
            name_key,
            values: Vec::new(),
            blocking_pop_streams: Vec::new(),
            blocking_until: SystemTime::now(),
        }
    }

    pub fn check_waiting_streams(&mut self) {
        for _ in 0..self.blocking_pop_streams.len() {
            if !self.values.is_empty() {
                let mut bl_stream = self.blocking_pop_streams.remove(0);
                let _ = bl_stream.write_all(
                    get_resp_from_string(&[self.name_key.clone(), self.values.remove(0)])
                        .as_bytes(),
                );
            } else {
                break;
            }
        }
    }
}
