use super::Storage;
use crate::error::*;
use crate::JsonStorageExt;
use ext::ResultExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DefaultRepository {
    db: Arc<dyn Storage>,
    table: String,
}

impl DefaultRepository {
    pub fn try_new(db: Arc<dyn Storage>, table: String) -> Result<Self> {
        db.ensure_table(table.as_str())?;
        Ok(Self { db, table })
    }

    pub fn get<T: DeserializeOwned>(&self, id: &str) -> Result<T> { self.db.get(self.table.as_str(), id).err_into() }

    pub fn put<T: Serialize + Debug>(&self, key: &str, op: &T) -> Result<()> {
        self.db.put(self.table.as_str(), key, op).err_into()
    }

    #[must_use]
    pub fn all<T: DeserializeOwned>(&self) -> Vec<T> {
        self.db
            .get_all::<T>(self.table.as_str())
            .map_or_else(|_| Vec::new(), |v| v.into_iter().map(|kv| kv.1).collect())
    }
}
