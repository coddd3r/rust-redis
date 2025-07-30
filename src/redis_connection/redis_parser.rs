use std::io::{prelude::*, Write};
use std::net::TcpStream;
use std::usize;

use std::io::ErrorKind;

#[derive(Debug)]
pub struct RedisConnection {
    pub stream: TcpStream,
    pub buffer: Vec<u8>,
    pub position: usize,
    pub offset: usize,
    pub prev_offset: usize,
    pub is_master: bool,
    pub multi_waiting: bool,
}

impl RedisConnection {
    //pub fn new(stream: Arc<Mutex<TcpStream>>) -> Self {
    pub fn new(stream: TcpStream) -> Self {
        stream.set_nonblocking(true).unwrap();
        RedisConnection {
            stream: stream,
            buffer: Vec::new(),
            position: 0,
            offset: 0,
            prev_offset: 0,
            is_master: false,
            multi_waiting: false,
        }
    }

    pub fn try_read_command(&mut self) -> std::io::Result<Option<Vec<Vec<String>>>> {
        // Read available data

        ////eprintln!("stream in read");
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
                //if !self.is_master {
                self.prev_offset = self.offset + 0;
                self.offset += n;
                eprintln!("current offset:{}", self.offset);
                //}
                self.buffer.extend_from_slice(&temp_buf[..n]);
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                return Ok(None);
            }
            Err(e) => {
                //eprintln!("BIG ERROR:{e}");
                return Err(e);
            }
        }
        //eprintln!("AFTER stream in read");

        // Parse complete commands from buffer
        self.parse_buffer()
    }

    fn parse_buffer(&mut self) -> std::io::Result<Option<Vec<Vec<String>>>> {
        eprintln!(
            "buffer as str:{:?}",
            String::from_utf8_lossy(&self.buffer[self.position..])
        );
        let mut commands = Vec::new();

        loop {
            eprintln!(
                "\n\n LOOP: handling buffer starting at pos:{}\n\n",
                self.position
            );
            eprintln!(
                "buffer as str:{:?}",
                String::from_utf8_lossy(&self.buffer[self.position..])
            );
            let mut lines = self.buffer[self.position..].split(|&b| b == b'\n');

            while let Some(line) = lines.next() {
                if line.is_empty() {
                    eprintln!("empty_line:{:?}", line);
                    continue;
                }
                self.position += line.len() + 1;
                let line_str = match String::from_utf8(line.to_vec()) {
                    Ok(s) => s,
                    Err(_) => {
                        continue;
                    } //if not valid utf8 keep going
                };

                match line_str.chars().next() {
                    // Resp array section
                    Some('*') => {
                        let arr_length = match line_str[1..].trim().parse::<usize>() {
                            Ok(n) => n,
                            Err(_) => {
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

                            //NOTE: might need to add + 1 for \n
                            self.position += line.len();

                            if !size_line.starts_with('$') {
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

                            let mut content = match lines.next() {
                                Some(line) => {
                                    self.position += line.len() + 1;

                                    match String::from_utf8(line.to_vec()) {
                                        Ok(s) => s,
                                        Err(_) => {
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
                            ////eprintln!("GOT content:{:?}", content);
                            //RESP ARRAY DECODED WRONG
                            if content.len() != size {
                                //    //eprintln!("breaking because content is not the same size");
                                break;
                            }

                            elements.push(content.to_string());
                        }
                        if valid && elements.len() == arr_length {
                            commands.push(elements);
                        }
                        //eprintln!(
                        //     "end of resp section, buf len:{}, pos:{}",
                        //     self.buffer.len(),
                        //     self.position
                        // );
                    }

                    Some('$') => {
                        // AT START OF RDB TRANSFER
                        //eprintln!("ACTUAL RDB SECTION");
                        let rdb_len = match line_str[1..].trim().parse::<usize>() {
                            Ok(n) => n,
                            Err(_) => {
                                continue;
                            }
                        };
                        // Skip RDB data

                        //eprintln!("found length {rdb_len}");
                        let rdb_start = self.position; // + line_str.len() + 2; // +1 for \r
                                                       //eprintln!("after, pos:{}", self.position);
                        let rdb_end = rdb_start + rdb_len;
                        //eprintln!(
                        //    "rdb start:{rdb_start} rdb_end:{rdb_end}, buffer length:{}",
                        //    self.buffer.len()
                        //);

                        let _rdb_bytes: Vec<_> = self.buffer[rdb_start..rdb_end].into();
                        //eprintln!(
                        //    "PARSED RDB IN STR:{:?}",
                        //    String::from_utf8_lossy(&rdb_bytes)
                        //);
                        //self.decode_rdb(rdb_bytes);

                        //eprintln!("POSITION AFTER RDB:{}", self.position);
                        self.position = rdb_end;

                        //eprintln!("\n\nafter rdb OFFSET:{}", self.offset);
                        //reset offset after handshake
                        self.offset = self.buffer.len() - self.position;
                        //eprintln!("after reset OFFSET:{}\n\n", self.offset);

                        break;
                    }
                    Some(_) => {
                        continue;
                    }
                    _ => continue,
                }
            }

            if (self.buffer.len() as i32) - (self.position as i32) <= 1 {
                eprintln!(
                    "breaking with length:{}, curr pos:{}",
                    self.buffer.len(),
                    self.position
                );
                break;
            } else {
                eprintln!(
                    "LOOPING AGAIN len:{}, pos:{}",
                    self.buffer.len(),
                    self.position
                );
            }
        }
        //eprintln!(
        //    "AFTER PARSE commands?{:?}, pos:{}, buffer len:{}",
        //    commands,
        //    self.position,
        //    self.buffer.len()
        //);

        Ok(Some(commands))
    }

    pub fn broadcast_command(&mut self, command: &[String]) {
        let s: Vec<&str> = command.iter().map(|e| e.as_str()).collect();
        let resp = self.format_resp_array(&s);
        self.write_to_stream(resp.as_bytes());
    }

    pub fn format_resp_array(&self, elements: &[&str]) -> String {
        let mut resp = format!("*{}\r\n", elements.len()); //.into_bytes();
        for element in elements {
            //resp.extend(format!("${}\r\n{}\r\n", element.len(), element).into_bytes());
            resp.push_str(&format!("${}\r\n{}\r\n", element.len(), element));
        }
        resp
    }

    pub fn write_to_stream(&mut self, buf: &[u8]) {
        //eprintln!(" stream in write");
        if let Ok(r) = String::from_utf8(buf.into()) {
            eprintln!("writing to stream: {r}")
        } else {
            eprintln!("WRITING RDB");
        };
        self.stream
            .write_all(&buf)
            .expect("in RespConn failed to write to steam");
        eprintln!("AFTER STREAM in write");
        //NOTE: removed int debugging xread block 0
        //
        // sleep(Duration::from_millis(5));
    }

    pub fn get_simple_str(&self, s: &str) -> String {
        format!("+{s}\r\n")
    }

    // pub fn decode_rdb(&self, received_rdb: Vec<u8>) {
    //     //eprintln!("DECODING RDB BYTES RECEIVED");
    //     //eprintln!(
    //         "read from stream num rdb file:{:?}, length:{:?}",
    //         received_rdb,
    //         received_rdb.len()
    //     );
    //     crate::print_hex_dump(&received_rdb);

    //     let received_rdb_path = std::env::current_dir().unwrap().join("dumpreceived.rdb");

    //     let mut file = File::create(&received_rdb_path).unwrap();
    //     file.write_all(&received_rdb)
    //         .expect("failed to write receive rdb to file");
    //     ////eprintln!("WRPTE RESPONSE TO FILE");
    //     let final_rdb = codecrafters_redis::read_rdb_file(received_rdb_path)
    //         .expect("failed tp read response rdb from file");
    //     //eprintln!("RECEIVED RDB:{:?}", final_rdb);
    // }
}
