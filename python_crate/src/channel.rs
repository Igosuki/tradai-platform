use brokers::prelude::Exchange;
use brokers::types::{SecurityType, Symbol};
use strategy::{MarketChannel, MarketChannelType};

#[pyclass(name = "Channel", module = "strategy", subclass)]
#[derive(Clone)]
#[pyo3(text_signature = "(source, exchange, pair, /)")]
pub struct PyChannel {
    source: String,
    exchange: String,
    pair: String,
}

#[pymethods]
impl PyChannel {
    /// The source may be one of 'orderbooks', 'trades', 'candles'
    /// Exchange must be one of the supported exchanges in lowercase (see platform documentation)
    /// Pair is a trading pair (to be later redefined as 'security')
    #[new]
    fn new(source: &str, exchange: &str, pair: &str) -> Self {
        Self {
            source: source.to_string(),
            exchange: exchange.to_string(),
            pair: pair.to_string(),
        }
    }
}

impl From<PyChannel> for MarketChannel {
    fn from(sc: PyChannel) -> Self {
        let market_channel_type = match sc.source.as_str() {
            "orderbooks" => MarketChannelType::Orderbooks,
            "trades" => MarketChannelType::Trades,
            "candles" => MarketChannelType::Candles,
            _ => unimplemented!(),
        };
        MarketChannel::builder()
            .r#type(market_channel_type)
            .symbol(Symbol::new(
                sc.pair.into(),
                SecurityType::Crypto,
                Exchange::from(sc.exchange),
            ))
            .build()
    }
}
