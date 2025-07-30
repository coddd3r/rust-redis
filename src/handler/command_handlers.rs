use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::redis_database::{Expiration, RdbError, RedisDatabase, RedisValue};

pub fn handle_set(
    k: String,
    v: String,
    new_db: &Arc<Mutex<RedisDatabase>>,
    expiry_info: Option<(&str, &str)>,
) -> Result<(), Box<RdbError>> {
    eprintln!("HANDLING SET FOR K:{k}, V:{v}");
    let mut use_insert = RedisValue {
        value: v,
        expires_at: None,
    };
    if let Some((expiry_type, expiry_time)) = expiry_info {
        match expiry_type {
            "px" => {
                let time_arg: u64 = expiry_time.parse().expect("failed to parse expiry time");
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64; ////eprintln!("got MILLISECONDS expiry:{time_arg}");
                let end_time_s = now + time_arg;
                ////eprintln!("AT: {now}, MSexpiry:{time_arg},end:{end_time_s}");
                let use_expiry = Some(Expiration::Milliseconds(end_time_s));
                use_insert.expires_at = use_expiry;
            }
            "ex" => {
                let time_arg: u32 = expiry_time.parse().expect("failed to parse expiry time");
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let end_time_s = now as u32 + time_arg;

                ////eprintln!("AT: {now}, got SECONDS expiry:{time_arg}, expected end:{end_time_s}");
                let use_expiry = Some(Expiration::Seconds(end_time_s as u32));
                use_insert.expires_at = use_expiry;
            }
            _ => {
                return Err(Box::new(RdbError::UnsupportedFeature(
                    "WRONG SET ARGUMENTS",
                )))
            }
        }
        ////eprintln!("before inserting in db, expiry:{:?}", use_expiry);
    }

    {
        eprintln!("IN HANDLE SET FUNCTION, BEFORE LOCK");
        let mut lk = new_db.lock().unwrap();
        lk.insert(k.clone(), use_insert);
        let res = lk.get(&k);
        eprintln!("IN HANDLE SET FUNCTION, AFTER LOCK GET RES: {:?}", res);
    }
    Ok(())
}
