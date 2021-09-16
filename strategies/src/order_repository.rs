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

    pub fn add_order(&self, _order: OrderDetail) -> Result<()> { Ok(()) }

    pub async fn get_order(&self, id: String) -> Result<OrderDetail> {
        let query = r#"select id, transaction_id, remote_id, status, exchange, pair, base_asset, quote_asset, side, order_type, enforcement, base_qty, quote_qty, price, stop_price, iceberg_qty, is_test, asset_type, executed_qty, cummulative_quote_qty, margin_side_effect, borrowed_amount, borrowed_asset, fills, weighted_price, total_executed_qty, rejection_reason as "rejection_reason: Json<RejectionReason>", created_at, updated_at, closed_at from orders where id = ?"#;
        Ok(sqlx::query_as!(OrderDetail, query)
            .bind(&id)
            .fetch_one(&self.pool)
            .await?)
    }

    pub fn orders_by_transaction_id(&self, _transaction_id: String) -> Result<()> { Ok(()) }

    pub fn orders_by(&self, _query: OrdersPaginatedQuery) -> Result<()> { Ok(()) }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_insert() {}
}
