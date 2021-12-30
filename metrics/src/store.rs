use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Mutex;

pub struct MetricStore<K, M> {
    inner: Mutex<HashMap<K, M>>,
}

impl<K, M> Default for MetricStore<K, M> {
    fn default() -> Self {
        Self {
            inner: Mutex::default(),
        }
    }
}

impl<K: Eq + Hash + Clone, M: Clone> MetricStore<K, M> {
    /// An immutable store that can insert a key only once
    #[must_use]
    pub fn new() -> Self { Self::default() }

    // TODO: don't know how to handle rwlock here (DashMap solves the pb)
    #[allow(clippy::missing_panics_doc)]
    pub fn get_or_create<F>(&self, key: K, provider: F) -> M
    where
        F: FnOnce() -> M,
    {
        let mut guard = self.inner.lock().unwrap();
        match guard.entry(key) {
            Entry::Occupied(o) => o.get().clone(),
            Entry::Vacant(e) => {
                let value = provider();
                e.insert(value.clone());
                value
            }
        }
    }
}
