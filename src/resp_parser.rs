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

    //pub fn add_connection(&mut self, stream: Arc<Mutex<TcpStream>>) -> usize {
    pub fn add_connection(&mut self, stream: TcpStream) -> usize {
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
    pub fn broadcast_command(&mut self, command: &[String]) {
        for conn in &mut self.connections {
            conn.broadcast_command(command)
            // Handle disconnection if needed
        }
    }
}

#[derive(Debug)]
pub struct RespConnection {
    //pub stream: Arc<Mutex<TcpStream>>,
    pub stream: TcpStream,
    pub buffer: Vec<u8>,
    pub position: usize,
}

impl RespConnection {
    //pub fn new(stream: Arc<Mutex<TcpStream>>) -> Self {
    pub fn new(stream: TcpStream) -> Self {
        // {
        //     let lk = stream.lock().unwrap();
        //     lk.set_nonblocking(true)
        //         .expect("Failed to set non-blocking");
        // }
        stream.set_nonblocking(true).unwrap();
        RespConnection {
            stream: stream,
            buffer: Vec::new(),
            position: 0,
        }
    }

    pub fn try_read_command(&mut self) -> std::io::Result<Option<Vec<Vec<String>>>> {
        // Read available data
        {
            //let mut stream = self.stream.lock().unwrap();
            let mut temp_buf = [0; 4096];

            match self.stream.read(&mut temp_buf) {
                Ok(0) => {
                    return Ok(None);
                }
                Ok(n) => {
                    eprintln!(
                        "Found {n} bytes, {:?}",
                        String::from_utf8(temp_buf[..n].into())
                    );
                    self.buffer.extend_from_slice(&temp_buf[..n]);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    return Ok(None);
                }
                Err(e) => {
                    eprintln!("BIG ERROR:{e}");
                    return Err(e);
                }
            }
        }
        eprintln!("AFTER stream in read");

        // Parse complete commands from buffer
        self.parse_buffer()
    }

    fn parse_buffer(&mut self) -> std::io::Result<Option<Vec<Vec<String>>>> {
        eprintln!("PARSING BUFFER");
        //let mut lines = self.buffer[self.position..].split(|&b| b == b'\n');
        if let Ok(parsed_string) = String::from_utf8(self.buffer[self.position..].into()) {
            let mut lines = parsed_string.split("\r\n");
            let mut commands = Vec::new();

            while let Some(line_str) = lines.next() {
                if line_str.is_empty() {
                    //eprintln!("EMPTY  BREAKING");
                    //break;
                    eprintln!("EMPTY CONTINUE");
                    continue;
                }

                // let line_str = match String::from_utf8(line.to_vec()) {
                //     Ok(s) => s,
                //     Err(_) => continue, // Skip invalid UTF-8
                // };

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
                            // let size_line = match lines.next() {
                            //     Some(line) => match String::from_utf8(line.to_vec()) {
                            //         Ok(s) => s,
                            //         Err(_) => {
                            //             valid = false;
                            //             break;
                            //         }
                            //     },
                            //     None => {
                            //         valid = false;
                            //         break;
                            //     }
                            // };

                            let size_line = lines.next().unwrap();
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

                            // let content = match lines.next() {
                            //     Some(line) => match String::from_utf8(line.to_vec()) {
                            //         Ok(s) => s,
                            //         Err(_) => {
                            //             valid = false;
                            //             break;
                            //         }
                            //     },
                            //     None => {
                            //         valid = false;
                            //         break;
                            //     }
                            // };
                            if let Some(content) = lines.next() {
                                if content.len() != size {
                                    valid = false;
                                    break;
                                }
                                elements.push(content.to_string());
                            };
                            //elements.push(content);
                        }

                        if valid && elements.len() == arr_length {
                            commands.push(elements);
                            self.position += line_str.len() + 1; // +1 for newline
                        } else {
                            eprintln!("valid?{valid}, elements?{:?}", elements);
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
                Some(commands)
            } else {
                None
            })
        } else {
            eprintln!("need to parse RDB");
            Ok(None)
        }
    }

    pub fn broadcast_command(&mut self, command: &[String]) {
        let s: Vec<&str> = command.iter().map(|e| e.as_str()).collect();
        let resp = self.format_resp_array(&s);
        // let mut stream = self.stream.lock().unwrap();
        //stream.write_all(&resp)
        self.write_to_stream(&resp);
    }

    pub fn format_resp_array(&self, elements: &[&str]) -> Vec<u8> {
        let mut resp = format!("*{}\r\n", elements.len()).into_bytes();
        for element in elements {
            resp.extend(format!("${}\r\n{}\r\n", element.len(), element).into_bytes());
        }
        resp
    }

    pub fn write_to_stream(&mut self, buf: &[u8]) {
        eprintln!(" stream in write");
        if let Ok(r) = String::from_utf8(buf.into()) {
            eprintln!("writing to stream: {r}")
        } else {
            eprintln!("WRITING RDB");
        };
        self.stream
            .write_all(&buf)
            .expect("in RespConn failed to write to steam");
        //std::thread::sleep(Duration::from_millis(10));
        eprintln!("AFTER STREAM in write");
    }
}
