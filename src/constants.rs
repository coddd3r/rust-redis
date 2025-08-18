//pub const ROLE: &str = "role";
//pub const MASTER: &str = "master";
//pub const SLAVE: &str = "slave";
//pub const MASTER_REPL_OFFSET: &str = "master_repl_offset";
//pub const MASTER_REPL_ID: &str = "master_replid";
//pub const REPL_CONF: &str = "REPLCONF";
//pub const GETACK: &str = "GETACK";
//pub const ACK: &str = "ACK";
//pub const LISTENING_PORT: &str = "listening-port";
//pub const PSYNC: &str = "PSYNC";
//pub const FULLRESYNC: &str = "FULLRESYNC";
//pub const DEFAULT_PORT: &str = "6379";
//pub const NOT_INT_ERROR: &[u8; 46] = b"-ERR value is not an integer or out of range\r\n";
//pub const EXEC_WITHOUT_MULTI: &[u8; 25] = b"-ERR EXEC without MULTI\r\n";
//pub const QUEUED_RESP: &[u8; 9] = b"+QUEUED\r\n";
//pub const PONG_RESPONSE: &str = b"+PONG\r\n";
//const RESP_OK: &[u8; 5] = b"+OK\r\n";
//const RESP_NULL: &[u8; 5] = b"$-1\r\n";
//const STRING: &[u8] = "+string\r\n".as_bytes();
//const NONE_TYPE: &[u8] = "+none\r\n".as_bytes();
//const ZERO_ERROR: &[u8] = b"-ERR The ID specified in XADD must be greater than 0-0\r\n";
//const SMALLER_ERROR: &[u8] =
//    b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
////////////////////////
pub const ROLE: &str = "role";
pub const MASTER: &str = "master";
pub const SLAVE: &str = "slave";
pub const MASTER_REPL_OFFSET: &str = "master_repl_offset";
pub const MASTER_REPL_ID: &str = "master_replid";
pub const REPL_CONF: &str = "REPLCONF";
pub const GETACK: &str = "GETACK";
pub const ACK: &str = "ACK";
pub const LISTENING_PORT: &str = "listening-port";
pub const PSYNC: &str = "PSYNC";
pub const FULLRESYNC: &str = "FULLRESYNC";
pub const DEFAULT_PORT: &str = "6379";
pub const NOT_INT_ERROR: &str = "-ERR value is not an integer or out of range\r\n";
pub const EXEC_WITHOUT_MULTI: &str = "-ERR EXEC without MULTI\r\n";
pub const QUEUED_RESP: &str = "+QUEUED\r\n";
pub const PONG_RESPONSE: &str = "+PONG\r\n";
pub const RESP_OK: &str = "+OK\r\n";
pub const RESP_NULL: &str = "$-1\r\n";
pub const ZERO_INT: &str = ":0\r\n";
pub const STRING: &str = "+string\r\n";
pub const NONE_TYPE: &str = "+none\r\n";
pub const ZERO_ERROR: &str = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
pub const SMALLER_ERROR: &str =
    "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
pub const EMPTY_ARRAY: &str = "*0\r\n";
pub const SUBCRIBED_ERROR:&str= "-ERR Can't execute 'echo': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n";
pub const ALLOWED_SUB_COMMANDS: [&str; 6] = [
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "PSUBSCRIBE",
    "PUNSUBSCRIBE",
    "PING",
    "QUIT",
];
