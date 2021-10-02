use datafusion::arrow::array::StructArray;
use strategies::coinnect_types::{LiveEvent, LiveEventEnvelope, Orderbook, Pair};
use strategies::Exchange;

pub mod avro_orderbook;
pub mod csv_orderbook;

fn get_col_as<'a, T: 'static>(sa: &'a StructArray, name: &str) -> &'a T {
    sa.column_by_name(name).unwrap().as_any().downcast_ref::<T>().unwrap()
}

fn live_order_book(
    exchange: Exchange,
    pair: Pair,
    ts: i64,
    asks: Vec<(f64, f64)>,
    bids: Vec<(f64, f64)>,
) -> LiveEventEnvelope {
    let orderbook = Orderbook {
        timestamp: ts,
        pair,
        asks,
        bids,
        last_order_id: None,
    };
    LiveEventEnvelope {
        xch: exchange,
        e: LiveEvent::LiveOrderbook(orderbook),
    }
}
