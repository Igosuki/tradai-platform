use std::sync::Arc;

use db::{Storage, StorageExt};
use ext::ResultExt;

use crate::error::*;
use crate::StrategyStatus;

pub trait DriverRepository {
    fn set_status(&self, status: StrategyStatus) -> Result<()>;

    fn get_status(&self) -> Result<Option<StrategyStatus>>;
}

pub(crate) struct GenericDriverRepository {
    db: Arc<dyn Storage>,
}

const DRIVER_TABLE: &str = "driver";

impl GenericDriverRepository {
    pub fn new(db: Arc<dyn Storage>) -> Self { Self { db } }
}

impl DriverRepository for GenericDriverRepository {
    fn set_status(&self, status: StrategyStatus) -> Result<()> {
        self.db.put(DRIVER_TABLE, "status", status)?;
        Ok(())
    }

    fn get_status(&self) -> Result<Option<StrategyStatus>> { self.db.get(DRIVER_TABLE, "status").err_into() }
}
