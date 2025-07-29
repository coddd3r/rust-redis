#[derive(Debug, Clone)]
pub struct RedisList {
    pub values: Vec<String>,
}

impl RedisList {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }
}
