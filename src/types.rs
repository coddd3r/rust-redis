use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct RedisValue {
    pub value: String,
    pub expires_at: Option<Expiration>,
}

#[derive(Debug, Clone)]
pub enum Expiration {
    Seconds(u32),
    Milliseconds(u64),
}

impl Expiration {
    pub fn as_seconds(&self) -> u64 {
        match self {
            Expiration::Seconds(secs) => *secs as u64,
            Expiration::Milliseconds(ms) => ms / 1000,
        }
    }

    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        eprintln!(
            "NOW: {now},checking expiry, seconds:{:?}",
            self.as_seconds()
        );
        self.as_seconds() <= now
    }
}

/// Represents a Redis database
#[derive(Debug, Default, Clone)]
pub struct RedisDatabase {
    pub data: HashMap<String, RedisValue>,
}

impl RedisDatabase {
    pub fn new() -> Self {
        RedisDatabase {
            data: HashMap::new(),
        }
    }
    pub fn insert(&mut self, key: String, value: RedisValue) {
        self.data.insert(key, value);
    }
    pub fn get(&self, key: &str) -> Option<&RedisValue> {
        self.data.get(key)
    }
}

/// Represents the complete RDB file structure
#[derive(Debug, Default)]
pub struct RdbFile {
    pub version: String,
    pub metadata: HashMap<String, String>,
    pub databases: HashMap<u8, RedisDatabase>,
}
