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

use codecrafters_redis::RedisDatabase;

use std::io::{BufRead, ErrorKind};

#[derive(Debug)]
pub struct BroadCastInfo {
    pub connections: Vec<RespConnection>,
    next_id: usize,
    pub ports: Vec<String>,
}

impl BroadCastInfo {
    pub fn new() -> Self {
        BroadCastInfo {
            connections: Vec::new(),
            next_id: 0,
            ports: Vec::new(),
        }
    }

    pub fn add_connection(&mut self, stream: Arc<Mutex<TcpStream>>) -> usize {
        let id = self.next_id;
        self.connections.push(RespConnection {
            //stream: Arc::new(Mutex::new(stream)),
            stream,
            buffer: Vec::new(),
            position: 0,
        });
        self.next_id += 1;
        id
    }
    pub fn broadcast_command(&self, command: &[String]) {
        for conn in &self.connections {
            if let Err(e) = conn.broadcast_command(command) {
                eprintln!("Broadcast failed: {}", e);
                // Handle disconnection if needed
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct RespConnection {
    pub stream: Arc<Mutex<TcpStream>>,
    pub buffer: Vec<u8>,
    pub position: usize,
}

impl RespConnection {
    pub fn new(stream: Arc<Mutex<TcpStream>>) -> Self {
        {
            let lk = stream.lock().unwrap();
            lk.set_nonblocking(true)
                .expect("Failed to set non-blocking");
        }
        RespConnection {
            stream: stream,
            buffer: Vec::new(),
            position: 0,
        }
    }

    pub fn try_read_command(&mut self) -> std::io::Result<Option<Vec<String>>> {
        // Read available data
        eprintln!("TRYING TO READ COMMAND");
        {
            eprintln!("locking stream in read");
            let mut stream = self.stream.lock().unwrap();
            let mut temp_buf = [0; 4096];

            match stream.read(&mut temp_buf) {
                Ok(0) => {
                    eprintln!("found nothing");
                    return Ok(None);
                }
                Ok(n) => {
                    eprintln!(
                        "Found {n} bytes, {:?}",
                        String::from_utf8_lossy(&temp_buf[..n])
                    );
                    self.buffer.extend_from_slice(&temp_buf[..n]);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    eprintln!("Error {e}");
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
        }
        eprintln!("AFTER locking stream in read");

        // Parse complete commands from buffer
        self.parse_buffer()
    }

    pub fn write_to_stream(&self, buf: &[u8]) {
        eprintln!("locking stream in write");
        let mut stream = self.stream.lock().unwrap();
        eprintln!("writing:{:?}", String::from_utf8_lossy(buf));
        stream
            .write_all(&buf)
            .expect("in RespConn failed to write to steam");
        eprintln!("AFTER STREAM LOCK in write");
    }
    fn parse_buffer(&mut self) -> std::io::Result<Option<Vec<String>>> {
        eprintln!("PARSING BUFFER");
        let mut lines = self.buffer[self.position..].split(|&b| b == b'\n');
        let mut commands = Vec::new();

        while let Some(line) = lines.next() {
            if line.is_empty() {
                continue;
            }

            let line_str = match String::from_utf8(line.to_vec()) {
                Ok(s) => s,
                Err(_) => continue, // Skip invalid UTF-8
            };

            match line_str.chars().next() {
                Some('*') => {
                    // Array type
                    let arr_length = match line_str[1..].trim().parse::<usize>() {
                        Ok(n) => n,
                        Err(_) => continue,
                    };

                    let mut elements = Vec::with_capacity(arr_length);
                    let mut valid = true;

                    for _ in 0..arr_length {
                        // Get bulk string header ($<length>)
                        let size_line = match lines.next() {
                            Some(line) => match String::from_utf8(line.to_vec()) {
                                Ok(s) => s,
                                Err(_) => {
                                    valid = false;
                                    break;
                                }
                            },
                            None => {
                                valid = false;
                                break;
                            }
                        };

                        if !size_line.starts_with('$') {
                            valid = false;
                            break;
                        }

                        // Get bulk string content
                        let size = match size_line[1..].trim().parse::<usize>() {
                            Ok(n) => n,
                            Err(_) => {
                                valid = false;
                                break;
                            }
                        };

                        let content = match lines.next() {
                            Some(line) => match String::from_utf8(line.to_vec()) {
                                Ok(s) => s,
                                Err(_) => {
                                    valid = false;
                                    break;
                                }
                            },
                            None => {
                                valid = false;
                                break;
                            }
                        };

                        if content.len() != size {
                            valid = false;
                            break;
                        }

                        elements.push(content);
                    }

                    if valid && elements.len() == arr_length {
                        commands.push(elements);
                        self.position += line_str.len() + 1; // +1 for newline
                    }
                }
                Some('$') => {
                    // Bulk string (RDB file transfer)
                    let rdb_len = match line_str[1..].trim().parse::<usize>() {
                        Ok(n) => n,
                        Err(_) => continue,
                    };

                    // Skip RDB data
                    let rdb_start = self.position + line_str.len() + 1;
                    let rdb_end = rdb_start + rdb_len + 2; // +2 for \r\n

                    if self.buffer.len() >= rdb_end {
                        self.position = rdb_end;
                    } else {
                        break; // Wait for more data
                    }
                }
                _ => continue, // Skip other RESP types
            }
        }

        Ok(if !commands.is_empty() {
            Some(commands.remove(0))
        } else {
            None
        })
    }

    pub fn broadcast_command(&self, command: &[String]) -> std::io::Result<()> {
        let s: Vec<&str> = command.iter().map(|e| e.as_str()).collect();
        let resp = self.format_resp_array(&s);
        let mut stream = self.stream.lock().unwrap();
        stream.write_all(&resp)
    }

    pub fn format_resp_array(&self, elements: &[&str]) -> Vec<u8> {
        let mut resp = format!("*{}\r\n", elements.len()).into_bytes();
        for element in elements {
            resp.extend(format!("${}\r\n{}\r\n", element.len(), element).into_bytes());
        }
        resp
    }
}
