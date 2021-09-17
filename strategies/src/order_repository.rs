use chrono::{DateTime, Utc};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::Connection;

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

struct OrderRepository<T: diesel::Connection> {
    conn: T,
}

impl OrderRepository<diesel::SqliteConnection> {
    pub async fn new(database_url: &str) -> Result<Self> {
        let conn = diesel::SqliteConnection::establish(&database_url)?;
        conn.batch_execute("
            PRAGMA journal_mode = WAL;          -- better write-concurrency
            PRAGMA synchronous = NORMAL;        -- fsync only in critical moments
            PRAGMA wal_autocheckpoint = 1000;   -- write WAL changes back every 1000 pages, for an in average 1MB WAL file. May affect readers if number is increased
            PRAGMA wal_checkpoint(TRUNCATE);    -- free some space by truncating possibly massive WAL files from the last run.
            PRAGMA busy_timeout = 5000;          -- sleep if the database is busy
            PRAGMA foreign_keys = ON;           -- enforce foreign keys
        ").map_err(diesel::ConnectionError::CouldntSetupConfiguration)?;
        Ok(Self { conn })
    }

    pub fn create_order(&self, order: OrderDetail) -> Result<OrderDetail> {
        todo!()
        //insert_into(orders::table).values(&order).get_result(&self.conn)?
    }

    pub fn update_order(&self, order: OrderDetail) -> Result<OrderDetail> {
        todo!()
        //update(orders.find(&order.id)).values(&order).get_result(&self.conn)?
    }

    pub async fn get_order(&self, id: String) -> Result<OrderDetail> {
        use crate::sql_schema::orders::dsl::*;
        Ok(orders.find(id).first(&self.conn)?)
        // Ok(sqlx::query_as!(OrderDetail, r#"select id, transaction_id, remote_id, status, exchange, pair, base_asset, quote_asset, side, order_type, enforcement, base_qty, quote_qty, price, stop_price, iceberg_qty, is_test, asset_type, executed_qty, cummulative_quote_qty, margin_side_effect, borrowed_amount, borrowed_asset, fills as "fills: Json<Vec<OrderFill>>", weighted_price, total_executed_qty, rejection_reason as "rejection_reason: Json<Rejection>", created_at, updated_at as "updated_at: DateTime<Utc>", closed_at from orders where id = ?"#, id)
        //     .fetch_one(&self.pool)
        //     .await?)
    }

    pub fn orders_by_transaction_id(&self, _transaction_id: String) -> Result<()> { Ok(()) }

    pub fn orders_by(&self, _query: OrdersPaginatedQuery) -> Result<()> { Ok(()) }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_insert() {}
}
