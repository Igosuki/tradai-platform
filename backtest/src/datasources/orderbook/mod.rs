use strategies::{Exchange, LiveEventEnvelope, MarketEvent, Orderbook, Pair};

pub mod convert;
pub mod csv_source;
pub mod raw_source;
pub mod sampled_source;

fn live_order_book(
    exchange: Exchange,
    pair: Pair,
    ts: i64,
    asks: Vec<(f64, f64)>,
    bids: Vec<(f64, f64)>,
) -> LiveEventEnvelope {
    let atom = pair.clone();
    let orderbook = Orderbook {
        timestamp: ts,
        pair,
        asks,
        bids,
        last_order_id: None,
    };
    LiveEventEnvelope {
        xch: exchange,
        pair: atom,
        e: MarketEvent::Orderbook(orderbook),
    }
}
