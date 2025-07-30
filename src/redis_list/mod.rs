use std::{net::TcpStream, time::SystemTime};

#[derive(Debug)]
pub struct RedisList {
    pub values: Vec<String>,
    pub blocking_pop_streams: Vec<TcpStream>,
    pub blocking_until: SystemTime,
}

impl RedisList {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            blocking_pop_streams: Vec::new(),
            blocking_until: SystemTime::now(),
        }
    }
}
