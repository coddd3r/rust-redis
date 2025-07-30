use std::net::TcpStream;
use std::time::SystemTime;
use std::usize;

use crate::redis_connection::RedisConnection;

#[derive(Debug)]
pub struct BroadCastInfo {
    pub connections: Vec<RedisConnection>,
    next_id: usize,
    pub ports: Vec<String>,
    pub num_waiting_for: usize,
    pub waiting_until: SystemTime,
    pub num_acks: usize,
}

impl BroadCastInfo {
    pub fn new() -> Self {
        BroadCastInfo {
            connections: Vec::new(),
            next_id: 0,
            ports: Vec::new(),
            num_waiting_for: 0,
            num_acks: 0,
            waiting_until: SystemTime::now(),
        }
    }

    //pub fn add_connection(&mut self, stream: Arc<Mutex<TcpStream>>) -> usize {
    pub fn add_connection(&mut self, stream: TcpStream) -> usize {
        let id = self.next_id;
        self.connections.push(RedisConnection::new(stream));
        self.next_id += 1;
        id
    }
    pub fn broadcast_command(&mut self, command: &[String]) {
        for conn in &mut self.connections {
            conn.broadcast_command(command)
            // Handle disconnection if needed
        }
    }
}
