use chrono::{DateTime, Utc};
use sqlx::{Pool, Sqlite, SqlitePool};

use crate::error::Result;
use crate::order_types::{OrderDetail, OrderStatus};

#[derive(Default)]
struct OrdersPaginatedQuery {
    exchange: Option<String>,
    pair: Option<String>,
    base_asset: Option<String>,
    quote_asset: Option<String>,
    limit: Option<u64>,
    offset: Option<u64>,
    updated_before: Option<DateTime<Utc>>,
    updated_after: Option<DateTime<Utc>>,
    statuses: Vec<OrderStatus>,
}

struct OrderRepository<T: sqlx::Database> {
    pool: Pool<T>,
}

impl OrderRepository<Sqlite> {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = SqlitePool::connect(database_url).await?;
        Ok(Self { pool })
    }

    pub fn add_order(_order: OrderDetail) -> Result<()> { Ok(()) }

    pub fn get_order(_order_id: String) -> Result<()> { Ok(()) }

    pub fn orders_by_transaction_id(_transaction_id: String) -> Result<()> { Ok(()) }

    pub fn orders_by(_query: OrdersPaginatedQuery) -> Result<()> { Ok(()) }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_insert() {}
}
