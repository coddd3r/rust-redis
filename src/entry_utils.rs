use crate::{entry_stream::RedisEntry, utils::get_bulk_string};

// Get stream name and seq id pairs
pub fn get_all_stream_names(lines: &[String]) -> Vec<(String, String)> {
    let mut use_vec = Vec::new();
    for (i, element) in lines.iter().enumerate() {
        if i == lines.len() - 1 {
            break;
        }
        if element.contains("-") && element.split('-').nth(0).unwrap().parse::<usize>().is_ok() {
            break;
        }
        use_vec.push(element);
    }
    let num_names = use_vec.len();
    let use_vec = use_vec
        .iter()
        .enumerate()
        .map(|(i, e)| (e.to_string(), lines[i + num_names].clone()))
        .collect();
    use_vec
}

pub fn get_xread_resp_array(v: &Vec<(String, Vec<(String, RedisEntry)>)>) -> Vec<u8> {
    if v.is_empty() {
        eprintln!("getting resp arr for empty");
        return crate::RESP_NULL.into();
    }

    //let mut resp = b"*1\r\n".to_vec();
    let mut resp: Vec<u8> = format!("*{}\r\n", v.len()).as_bytes().into();
    for (stream_name, id_entry) in v {
        resp.extend(b"*2\r\n");
        resp.extend(&get_bulk_string(stream_name));
        resp.extend(b"*1\r\n");
        id_entry.iter().for_each(|(entry_id, ent)| {
            resp.extend(b"*2\r\n");
            resp.extend(get_bulk_string(&entry_id));
            resp.extend(ent.entry_resp_array());
        });
    }
    resp
}
