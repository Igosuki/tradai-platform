use std::collections::HashMap;

pub fn url_encode_hashmap<S: ::std::hash::BuildHasher>(hashmap: &HashMap<&str, &str, S>) -> String {
    if hashmap.is_empty() {
        return String::new();
    }
    let mut acc = String::new();
    for (name, param) in hashmap {
        acc += &format!("{}={}&", name, param);
    }
    acc.pop(); // remove the last "&"
    acc
}

pub fn strip_empties<S: ::std::hash::BuildHasher>(map: &mut HashMap<&str, &str, S>) {
    map.retain(|_, v| !v.is_empty());
}
