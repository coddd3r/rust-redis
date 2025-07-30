use std::net::TcpStream;

#[derive(Debug)]
pub struct Channel {
    pub name: String,
    //pub subscribers: Vec<Subscriber>,
    pub subscribers: Vec<TcpStream>,
}

impl Channel {
    pub fn new(name: &String) -> Self {
        Channel {
            name: name.clone(),
            subscribers: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Subscriber {
    pub sub_id: String,
    //pub channels: Vec<Channel>,
    pub channel_count: usize,
}

impl Subscriber {
    pub fn new(port: String) -> Self {
        Subscriber {
            sub_id: port,
            channel_count: 0,
        }
    }
}
