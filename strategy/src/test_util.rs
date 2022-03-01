use std::sync::Arc;

use db::{get_or_create, DbOptions, Storage};

pub fn test_db() -> Arc<dyn Storage> {
    let path = util::test::test_dir();
    get_or_create(&DbOptions::new(path), "", vec![])
}

pub mod fixtures {
    #![allow(dead_code)]

    use brokers::exchange::Exchange;
    use brokers::prelude::MarketEventEnvelope;
    use brokers::types::{MarketEvent, Orderbook, SecurityType, Symbol};
    use util::time::now;

    static DEFAULT_PAIR: &str = "BTC_USDT";
    static DEFAULT_EXCHANGE: Exchange = Exchange::Binance;

    pub(crate) fn default_order_book_event() -> MarketEventEnvelope {
        MarketEventEnvelope::new(
            Symbol::new(DEFAULT_PAIR.into(), SecurityType::Crypto, DEFAULT_EXCHANGE),
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
