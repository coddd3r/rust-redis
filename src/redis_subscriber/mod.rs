#[derive(Debug)]
pub struct Channel {
    name: String,
    subscribers: Vec<Subscriber>,
}

impl Channel {
    pub fn new(name: &String) -> Self {
        Channel {
            name: name.clone(),
            subscribers: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct Subscriber {
    sub_id: String,
    channels: Vec<Channel>,
}

impl Subscriber {
    pub fn new(port: String) -> Self {
        Subscriber {
            sub_id: port,
            channels: Vec::new(),
        }
    }
}
