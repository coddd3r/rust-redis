use std::collections::HashMap;
use std::error::Error;
use std::fs::{self, File};
use std::io::{prelude::*, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::str::from_utf8;
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

        //let mut stream = self.stream.lock().unwrap();
        let mut temp_buf = [0; 4096];

        match self.stream.read(&mut temp_buf) {
            Ok(0) => {
                std::thread::sleep(Duration::from_millis(10));
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
                std::thread::sleep(Duration::from_millis(20));
                return Ok(None);
            }
            Err(e) => {
                eprintln!("BIG ERROR:{e}");
                return Err(e);
            }
        }
        eprintln!("AFTER stream in read");

        // Parse complete commands from buffer
        self.parse_buffer()
    }

    fn parse_buffer(&mut self) -> std::io::Result<Option<Vec<Vec<String>>>> {
        eprintln!("PARSING BUFFER, starting at pos:{}", self.position);
        eprintln!("CURR BUFFER:{:?}", String::from_utf8_lossy(&self.buffer));
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
                            eprintln!(
                                "adding to pos after valid line with elements before: {}",
                                self.position
                            );
                            self.position += line_str.len() + 1; // +1 for newline
                            eprintln!("after:{}", self.position);
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
                        let rdb_end = rdb_start + rdb_len + 4; // +2 for \r\n

                        if self.buffer.len() >= rdb_end {
                            eprintln!("SHOULDNT BE MOVING RDB END");
                            self.position = rdb_end;
                            eprintln!("AFTER MOVING ILLEGAL RDB END:{}", self.position);
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
            eprintln!(
                "need to parse RDB, got string:{:?}",
                String::from_utf8_lossy(&self.buffer[self.position..])
            );
            self.handle_rdb_transfer()
        }
    }

    fn handle_rdb_transfer(&mut self) -> std::io::Result<Option<Vec<Vec<String>>>> {
        eprintln!("\n\nhandling rdb starting at pos:{}\n\n", self.position);
        let mut lines = self.buffer[self.position..].split(|&b| b == b'\n');
        let mut commands = Vec::new();

        while let Some(line) = lines.next() {
            if line.is_empty() {
                //move past '\n'
                eprint!("adding 1 to pos for empty line, before {}", self.position);
                self.position += 1;
                eprintln!("after:{}", self.position);
                continue;
            }

            eprintln!("checking line:{:?}", String::from_utf8_lossy(line.into()));
            let line_str = match String::from_utf8(line.to_vec()) {
                Ok(s) => s,
                Err(_) => {
                    eprint!(
                        "adding 1 to pos for error to utf8 line, before {}",
                        self.position
                    );
                    self.position += line.len() + 1;
                    eprintln!("after pos:{}", self.position);

                    continue;
                } //if not valid utf8 keep going
            };

            eprintln!("line str valid {:?}", line_str);
            match line_str.chars().next() {
                Some('*') => {
                    eprintln!("\n\nMATCHED A LINE IN RDB!!???{:?}\n\n", line_str);
                    let arr_length = match line_str[1..].trim().parse::<usize>() {
                        Ok(n) => n,
                        Err(_) => {
                            eprint!(
                                "adding 1 to pos for error to usize line, before {}",
                                self.position
                            );
                            self.position += line_str.len() + 1;
                            eprintln!("after pos:{}", self.position);
                            continue;
                        }
                    };
                    let mut elements = Vec::with_capacity(arr_length);
                    let mut valid = true;

                    for _ in 0..arr_length {
                        let size_line = match lines.next() {
                            Some(line) => match String::from_utf8(line.into()) {
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
                            eprintln!("FAKE SIZE LINE");
                            valid = false;
                            break;
                        }

                        let size = match size_line[1..].trim().parse::<usize>() {
                            Ok(n) => n,
                            Err(_) => {
                                valid = false;
                                break;
                            }
                        };
                        eprintln!("in resp got size:{size}, from size_line:{:?}", size_line);
                        let mut content = match lines.next() {
                            Some(line) => {
                                match String::from_utf8(line.to_vec()) {
                                    Ok(s) => s,
                                    Err(_) => {
                                        eprintln!("breaking in conversion of line to utf8 insdie resp arr");
                                        valid = false;
                                        break;
                                    }
                                }
                            }
                            None => {
                                valid = false;
                                break;
                            }
                        };

                        content = content.trim().to_string();
                        eprintln!("GOT content:{:?}", content);
                        //RESP ARRAY DECODED WRONG
                        if content.len() != size {
                            eprintln!("breaking because content is not the same size");
                            valid = false;
                            break;
                        }

                        eprintln!("ELEMENTs at end: {:?}", elements);
                        elements.push(content.to_string());
                    }
                    if valid && elements.len() == arr_length {
                        commands.push(elements);
                        eprint!(
                            "adding length + 1 for valide elements, pos, {}",
                            self.position
                        );
                        self.position += line_str.len() + 1; // +1 for newline
                        eprintln!("after, pos:{}", self.position)
                    }
                }
                Some('$') => {
                    // AT START OF RDB TRANSFER
                    eprintln!("ACTUAL RDB SECTION");
                    let rdb_len = match line_str[1..].trim().parse::<usize>() {
                        Ok(n) => n,
                        Err(_) => {
                            eprint!(
                                "actual rdb adding line length in failed usize parse, before:{}",
                                self.position
                            );
                            self.position += line_str.len() + 1;
                            eprintln!("after, pos:{}", self.position);
                            continue;
                        }
                    };
                    // Skip RDB data

                    eprintln!("found length {rdb_len}");
                    eprintln!(
                        "adding to length for final line, before pos{:?}",
                        self.position
                    );
                    let rdb_start = self.position + line_str.len() + 4; // +2 for \r\n
                    eprintln!("after, pos:{}", self.position);
                    let rdb_end = rdb_start + rdb_len; // + 2;
                    eprintln!(
                        "rdb start:{rdb_start} rdb_end:{rdb_end}, buffer length:{}",
                        self.buffer.len()
                    );

                    let rdb_bytes: Vec<_> = self.buffer[rdb_start..rdb_end].into();
                    eprintln!(
                        "PARSED RDB IN STR:{:?}",
                        String::from_utf8_lossy(&rdb_bytes)
                    );
                    //self.decode_rdb(rdb_bytes);

                    if self.buffer.len() >= rdb_end {
                        self.position = rdb_end; // Move pointer forward
                        eprintln!("POSITION AFTER RDB:{}", self.position);
                        break;
                    } else {
                        eprintln!("\n\nBREAK??\n\n");
                        break; // Wait for more data
                    }
                }
                _ => {
                    eprint!("\nadding to pos in other curr:{}", self.position);
                    self.position += line_str.len();
                    eprintln!(" other NEXT:{}\n\n\n", self.position);
                    continue;
                }
            }
        }
        eprintln!(
            "AFTER RDB commands?{:?}, pos:{}, buffer len:{}",
            commands,
            self.position,
            self.buffer.len()
        );
        Ok(Some(commands))
    }
    pub fn broadcast_command(&mut self, command: &[String]) {
        eprintln!("got signal to propagate to stream:{:?}", self.stream);
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
        eprintln!("AFTER STREAM in write");
    }

    pub fn decode_rdb(&self, received_rdb: Vec<u8>) {
        eprintln!("DECODING RDB BYTES RECEIVED");
        eprintln!(
            "read from stream num rdb file:{:?}, length:{:?}",
            received_rdb,
            received_rdb.len()
        );
        crate::print_hex_dump(&received_rdb);

        let received_rdb_path = std::env::current_dir().unwrap().join("dumpreceived.rdb");

        let mut file = File::create(&received_rdb_path).unwrap();
        file.write_all(&received_rdb)
            .expect("failed to write receive rdb to file");
        //eprintln!("WRPTE RESPONSE TO FILE");
        let final_rdb = codecrafters_redis::read_rdb_file(received_rdb_path)
            .expect("failed tp read response rdb from file");
        eprintln!("RECEIVED RDB:{:?}", final_rdb);
    }
}
