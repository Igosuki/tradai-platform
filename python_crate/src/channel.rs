use brokers::prelude::Exchange;
use brokers::types::{MarketChannel, MarketChannelType, SecurityType, Symbol};
use chrono::Duration;
use stats::kline::{Resolution, TimeUnit};
use std::str::FromStr;

#[pyclass(name = "Channel", module = "strategy", subclass)]
#[derive(Clone)]
#[pyo3(
    text_signature = "(source, exchange, pair, time_unit='minute', units=1, tick_rate_millis=200, only_final=True, /)"
)]
pub struct PyChannel {
    source: String,
    exchange: String,
    pair: String,
    time_unit: Option<String>,
    units: Option<u32>,
    tick_rate_millis: Option<i64>,
    only_final: Option<bool>,
}

#[pymethods]
impl PyChannel {
    /// The source may be one of 'orderbooks', 'trades', 'candles'
    /// Exchange must be one of the supported exchanges in lowercase (see platform documentation)
    /// Pair is a trading pair (to be later redefined as 'security')
    #[new]
    fn new(
        source: &str,
        exchange: &str,
        pair: &str,
        time_unit: Option<String>,
        units: Option<u32>,
        tick_rate_millis: Option<i64>,
        only_final: Option<bool>,
    ) -> Self {
        Self {
            source: source.to_string(),
            exchange: exchange.to_string(),
            pair: pair.to_string(),
            time_unit,
            units,
            tick_rate_millis,
            only_final,
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
                Exchange::from_str(&sc.exchange).unwrap(),
            ))
            .resolution(sc.time_unit.map(|time_unit| {
                Resolution::new(TimeUnit::from_str(time_unit.as_str()).unwrap(), sc.units.unwrap_or(1))
            }))
            .tick_rate(sc.tick_rate_millis.map(|tr| (Duration::milliseconds(tr))))
            .only_final(sc.only_final)
            .build()
    }
}
