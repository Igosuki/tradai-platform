use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Mutex;

pub struct MetricStore<K, M> {
    inner: Mutex<HashMap<K, M>>,
}

impl<K: Eq + Hash + Clone, M: Clone> MetricStore<K, M> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }
    pub fn get_or_create<F>(&self, key: K, provider: F) -> M
    where
        F: FnOnce() -> M,
    {
        let mut guard = self.inner.lock().unwrap();
        match guard.entry(key.clone()) {
            Entry::Occupied(_) => {}
            Entry::Vacant(e) => {
                e.insert(provider());
            }
        }
        guard.get(&key).unwrap().clone()
    }
}
