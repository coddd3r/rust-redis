use std::collections::HashMap;
use std::error::Error;
use std::fs::{self, File};
use std::io::{prelude::*, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
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
                //std::thread::sleep(Duration::from_millis(5));
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
                //std::thread::sleep(Duration::from_millis(5));
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
        eprintln!("\n\nhandling buffer starting at pos:{}\n\n", self.position);
        let mut lines = self.buffer[self.position..].split(|&b| b == b'\n');
        let mut commands = Vec::new();

        while let Some(line) = lines.next() {
            if line.is_empty() {
                //move past '\n'
                eprint!(
                    "adding 1 to pos for empty line:{:?}, before {}",
                    String::from_utf8_lossy(line.into()),
                    self.position
                );
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
                    eprintln!("\n\nMATCHED A RESP LINE!!{:?}\n\n", line_str);
                    let arr_length = match line_str[1..].trim().parse::<usize>() {
                        Ok(n) => n,
                        Err(_) => {
                            eprint!(
                                "adding  pos for error in usize line, before {}",
                                self.position
                            );
                            self.position += line_str.len() + 1;
                            eprintln!("after pos:{}", self.position);
                            continue;
                        }
                    };
                    let mut elements = Vec::with_capacity(arr_length);
                    let mut valid = true;

                    eprintln!(
                        "adding to pos for arr length line:{:?} in resp arr before: {}",
                        line_str, self.position
                    );
                    self.position += line_str.len() + 1; // +2 for \r\n since we split at CRLF
                    eprintln!("after:{}", self.position);

                    eprintln!(
                        "adding to pos for each element's new line removed in the spli in resp arr:{:?} before: {}",
                                arr_length, self.position
                    );
                    self.position += arr_length * 2 - 1; // +1 for \r
                    eprintln!("after:{}", self.position);

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

                        eprintln!(
                            "adding to pos for size line in resp arr:{:?} before: {}",
                            size_line, self.position
                        );
                        self.position += size_line.len(); // +1 for \r
                        eprintln!("after:{}", self.position);

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
                        eprintln!(
                            "adding to pos for content line in resp arr:{:?} before: {}",
                            content, self.position
                        );
                        self.position += content.len(); // +1 for \n

                        eprintln!("after:{}", self.position);

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
                        // eprint!(
                        //     "adding length + 1 for valide elements, pos, {}",
                        //     self.position
                        // );
                        // self.position += line_str.len() + 1; // +1 for newline
                        // eprintln!("after, pos:{}", self.position)
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
                    let rdb_start = self.position + line_str.len() + 1; // +2 for \r\n
                    eprintln!("after, pos:{}", self.position);
                    let rdb_end = rdb_start + rdb_len;
                    eprintln!(
                        "rdb start:{rdb_start} rdb_end:{rdb_end}, buffer length:{}",
                        self.buffer.len()
                    );

                    let rdb_bytes: Vec<_> = self.buffer[rdb_start..rdb_end].into();
                    eprintln!(
                        "PARSED RDB IN STR:{:?}",
                        String::from_utf8_lossy(&rdb_bytes)
                    );
                    self.decode_rdb(rdb_bytes);

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
        if self.position < self.buffer.len() {
            self.parse_buffer()
        } else {
            Ok(Some(commands))
        }
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
        sleep(Duration::from_millis(5));
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
