pub mod fixtures {
    #![allow(dead_code)]

    use brokers::exchange::Exchange;
    use brokers::types::{MarketEvent, MarketEventEnvelope, Orderbook, SecurityType, Symbol};
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
