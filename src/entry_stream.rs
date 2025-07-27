use std::collections::HashMap;

use crate::utils::get_bulk_string;

#[derive(Debug, Default, Clone)]
pub struct EntryStream {
    pub entries: HashMap<String, (String, String)>,
    pub last_id: (usize, usize),
    pub sequences: HashMap<usize, usize>,
}

const ZERO_ERROR: &[u8] = b"-ERR The ID specified in XADD must be greater than 0-0\r\n";
const SMALLER_ERROR: &[u8] =
    b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";

impl EntryStream {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            last_id: (0, 0),
            sequences: HashMap::new(),
        }
    }
    //pub fn is_valid_id(&mut self, id: &str) -> bool {
    //}

    pub fn get_next_sequence(&mut self, seq: usize) -> usize {
        let ret = self.sequences.entry(seq).or_insert(0).clone();
        *(self.sequences.get_mut(&seq).unwrap()) += 1;
        ret
    }
    pub fn stream_id_response(&mut self, id: &str) -> (bool, Vec<u8>) {
        let mut parts: Vec<_> = Vec::new();

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
            self.last_id = (parts[0], parts[1]);
            let use_id = format!("{}-{}", parts[0], parts[1]);
            eprintln!("returning id:{use_id}");
            return (true, get_bulk_string(&use_id));
        }
        return (false, SMALLER_ERROR.into());

        // match format!("{}{}", parts[0], parts[1]).parse::<usize>() {
        //     Ok(s) => {
        //         eprintln!("found num:{s}");
        //         if s < 1 {
        //             eprintln!("got 0");
        //             return (false, ZERO_ERROR.into());
        //         }
        //         if self.last_id < s {
        //             self.last_id = s;
        //             return (true, get_bulk_string(id));
        //         }
        //         return (false, SMALLER_ERROR.into());
        //     }
        //     Err(_) => {
        //         return (false, SMALLER_ERROR.into());
        //     }
        // }
    }
}
