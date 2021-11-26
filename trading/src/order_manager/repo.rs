use super::error::*;
use crate::order_manager::types::OrderDetail;
use db::{Storage, StorageExt};
use ext::ResultExt;
use std::sync::Arc;

pub(super) static ORDERS_TABLE: &str = "orders";

#[derive(Debug, Clone)]
pub struct OrderRepository {
    db: Arc<dyn Storage>,
}

impl OrderRepository {
    pub(crate) fn new(db: Arc<dyn Storage>) -> Self { Self { db } }

    pub(crate) fn get(&self, id: &str) -> Result<OrderDetail> { self.db.get(ORDERS_TABLE, id).err_into() }

    #[allow(dead_code)]
    pub(crate) fn all(&self) -> Result<Vec<(Box<[u8]>, OrderDetail)>> { self.db.get_all(ORDERS_TABLE).err_into() }

    #[tracing::instrument(skip(self), level = "info")]
    pub(crate) fn put(&self, order: OrderDetail) -> Result<()> {
        self.db.put(ORDERS_TABLE, &order.id.clone(), order).err_into()
    }
}
