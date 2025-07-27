use std::collections::HashMap;

use crate::utils::get_bulk_string;

#[derive(Debug, Default, Clone)]
pub struct EntryStream {
    pub entries: HashMap<String, (String, String)>,
    pub last_id: usize,
}

const ZERO_ERROR: &[u8] = b"-ERR The ID specified in XADD must be greater than 0-0\r\n";
const SMALLER_ERROR: &[u8] =
    b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";

impl EntryStream {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            last_id: 0,
        }
    }
    //pub fn is_valid_id(&mut self, id: &str) -> bool {
    //}

    pub fn stream_id_response(&mut self, id: &str) -> (bool, Vec<u8>) {
        let parts: Vec<_> = id
            .split("-")
            .into_iter()
            .map(|e| e.parse::<usize>().unwrap())
            .collect();

        match format!("{}{}", parts[0], parts[1]).parse::<usize>() {
            Ok(s) => {
                if s < 1 {
                    return (false, ZERO_ERROR.into());
                }
                if self.last_id > s {
                    self.last_id = s;
                    return (true, get_bulk_string(id));
                }
                return (false, SMALLER_ERROR.into());
            }
            Err(_) => {
                return (false, SMALLER_ERROR.into());
            }
        }
    }
}
