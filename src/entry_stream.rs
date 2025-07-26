use std::collections::HashMap;

#[derive(Debug, Default, Clone)]
pub struct EntryStream {
    pub entries: HashMap<String, (String, String)>,
    pub last_id: usize,
}

impl EntryStream {
    pub fn is_valid_id(&mut self, id: &str) -> bool {
        let parts: Vec<_> = id
            .split("-")
            .into_iter()
            .map(|e| e.parse::<usize>().unwrap())
            .collect();
        match format!("{}{}", parts[0], parts[1]).parse::<usize>() {
            Ok(s) => {
                if self.last_id > s {
                    self.last_id = s;
                    return true;
                }
                false
            }
            Err(_) => {
                return false;
            }
        }
    }
}
