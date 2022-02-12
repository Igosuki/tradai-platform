use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::hash::Hash;

pub struct MetricStore<K, M> {
    inner: DashMap<K, M>,
}

impl<K: Eq + Hash + Clone, M: Clone> MetricStore<K, M> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            inner: DashMap::default(),
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn get_or_create<F>(&self, key: K, provider: F) -> M
    where
        F: FnOnce() -> M,
    {
        let guard = &self.inner;
        match guard.entry(key.clone()) {
            Entry::Occupied(_) => {}
            Entry::Vacant(e) => {
                e.insert(provider());
            }
        }
        guard.get(&key).unwrap().clone()
    }
}
