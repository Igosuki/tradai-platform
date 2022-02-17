use db::Storage;
use std::sync::Arc;

#[derive(Clone)]
#[pyclass(name = "Storage")]
pub(crate) struct PyDb {
    inner: Arc<dyn Storage>,
}

impl PyDb {
    pub(crate) fn db(&self) -> Arc<dyn Storage> { self.inner.clone() }
}

impl From<Arc<dyn Storage>> for PyDb {
    fn from(inner: Arc<dyn Storage>) -> Self { Self { inner } }
}
