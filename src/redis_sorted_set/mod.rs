use std::collections::HashMap;

#[derive(Debug, Default, Clone)]
pub struct UserScore {
    score: f64,
    name: String,
}

impl UserScore {
    pub fn new(score: f64, name: String) -> Self {
        Self { score, name }
    }
}

impl PartialOrd for UserScore {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            self.score
                .total_cmp(&other.score)
                .then(self.name.cmp(&other.name)),
        )
    }
}

impl PartialEq for UserScore {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.name == other.name
    }
}

pub struct RedisSortedSet {
    collection: Vec<UserScore>,
    user_map: HashMap<String, f64>,
}

impl RedisSortedSet {
    pub fn new() -> Self {
        Self {
            collection: Vec::new(),
            user_map: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.collection.len()
    }

    pub fn insert(&mut self, score: &str, name: &str) -> bool {
        let new_score = score.parse::<f64>().unwrap();

        if let Some((name, old_score)) = self.user_map.remove_entry(name) {
            eprintln!("FOUND EXISTING");
            let user_score = UserScore {
                name: name.clone(),
                score: old_score,
            };
            let pos = get_pos(&self.collection, &user_score);
            eprintln!("REMOVING AT:{pos}");
            self.collection.remove(pos);
            self.insert(score, &name);
            false
        } else {
            let name = name.to_string();
            let user_score = UserScore {
                score: new_score,
                name: name.clone(),
            };
            let pos = bin_search(&self.collection, &user_score);
            eprintln!("pos:{pos}");
            self.collection.splice(pos..pos, [user_score]);
            eprintln!("after insert, collection:{:?}", self.collection);
            self.user_map.insert(name, new_score);
            eprintln!("map after insert:{:?}", self.user_map);
            true
        }
    }

    pub fn rank(&self, member_name: &str) -> Option<usize> {
        if let Some(score) = self.user_map.get(member_name) {
            let user_score = UserScore {
                score: *score,
                name: member_name.to_string(),
            };
            let pos = get_pos(&self.collection, &user_score);
            Some(pos)
        } else {
            None
        }
    }
}

fn bin_search(sub_vec: &[UserScore], search_item: &UserScore) -> usize {
    eprintln!("in bin search sub vec{sub_vec:?},");
    let use_len = sub_vec.len();
    if use_len == 0 {
        return 0;
    }

    let mid = use_len / 2;
    let mid_elem = &sub_vec[mid];
    eprintln!(" using mid:{mid}, mid elem:{mid_elem:?}");
    let res = {
        if mid_elem > search_item {
            eprintln!("{:?} is greater than {:?}", sub_vec[mid], search_item);
            bin_search(&sub_vec[0..mid], search_item)
        } else if mid_elem == search_item {
            mid
        } else {
            eprintln!("{:?} is less than {:?}", sub_vec[mid], search_item);
            mid + 1 + bin_search(&sub_vec[mid + 1..], search_item)
        }
    };
    eprintln!("final return res:{res}");
    res
}

fn get_pos(sub_vec: &[UserScore], search_item: &UserScore) -> usize {
    let pos = bin_search(sub_vec, search_item);
    eprintln!("IN GET POS returning position: {pos}");
    pos
}
