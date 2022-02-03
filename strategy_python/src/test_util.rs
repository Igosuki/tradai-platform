pub mod fixtures {
    #![allow(dead_code)]

    use coinnect_rt::exchange::Exchange;
    use coinnect_rt::types::{MarketEvent, MarketEventEnvelope, Orderbook};
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
