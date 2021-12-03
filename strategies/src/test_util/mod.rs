#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use db::{get_or_create, DbOptions, Storage};

#[cfg(test)]
pub mod draw;
#[cfg(test)]
pub mod event;
#[cfg(test)]
pub mod fs;
#[cfg(test)]
pub mod input;
#[cfg(test)]
pub mod log;

pub fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

pub fn test_db() -> Arc<dyn Storage> {
    let path = util::test::test_dir();
    get_or_create(&DbOptions::new(path), "", vec![])
}

#[cfg(test)]
pub fn test_db_with_path<S: AsRef<Path>>(path: S) -> Arc<dyn Storage> {
    let options = DbOptions::new(path);
    get_or_create(&options, "", vec![])
}

pub mod fixtures {
    #![allow(dead_code)]

    use coinnect_rt::exchange::Exchange;
    use coinnect_rt::prelude::MarketEventEnvelope;
    use coinnect_rt::types::{MarketEvent, Orderbook};
    use util::time::now;

    static DEFAULT_PAIR: &str = "BTC_USDT";
    static DEFAULT_EXCHANGE: Exchange = Exchange::Binance;

    pub(crate) fn default_order_book_event() -> MarketEventEnvelope {
        MarketEventEnvelope::new(
            DEFAULT_EXCHANGE,
            DEFAULT_PAIR.into(),
            MarketEvent::Orderbook(Orderbook {
                pair: DEFAULT_PAIR.into(),
                asks: vec![(1.0, 10.0)],
                bids: vec![(1.0, 11.0)],
                timestamp: now().timestamp_millis(),
                last_order_id: None,
            }),
        )
    }
}
