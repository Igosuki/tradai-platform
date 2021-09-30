use crate::error::*;
use db::{Storage, StorageExt};
use ext::ResultExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::sync::Arc;

static OPERATIONS_TABLE: &str = "orders";

#[derive(Debug, Clone)]
pub(crate) struct OperationsRepository {
    db: Arc<dyn Storage>,
}

impl OperationsRepository {
    pub(crate) fn new(db: Arc<dyn Storage>) -> Self {
        db.ensure_table(OPERATIONS_TABLE).unwrap();
        Self { db }
    }

    pub(crate) fn get<T: DeserializeOwned>(&self, id: &str) -> Result<T> {
        self.db.get(OPERATIONS_TABLE, id).err_into()
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub(crate) fn put<T: Serialize + Debug>(&self, key: &str, op: &T) -> Result<()> {
        self.db.put(OPERATIONS_TABLE, key, op).err_into()
    }

    pub(crate) fn all<T: DeserializeOwned>(&self) -> Vec<T> {
        self.db
            .get_all::<T>(OPERATIONS_TABLE)
            .map(|v| v.into_iter().map(|kv| kv.1).collect())
            .unwrap_or_else(|_| Vec::new())
    }
}
