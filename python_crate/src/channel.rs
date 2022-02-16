use brokers::prelude::Exchange;
use strategy::Channel;

#[pyclass(name = "Channel", module = "strategy", subclass)]
#[derive(Clone)]
#[pyo3(text_signature = "Channel(source, exchange, pair, /)")]
pub struct PyChannel {
    source: String,
    exchange: String,
    pair: String,
}

#[pymethods]
impl PyChannel {
    #[new]
    fn new(source: &str, exchange: &str, pair: &str) -> Self {
        Self {
            source: source.to_string(),
            exchange: exchange.to_string(),
            pair: pair.to_string(),
        }
    }
}

impl From<PyChannel> for Channel {
    fn from(sc: PyChannel) -> Self {
        match sc.source.as_str() {
            "orderbooks" => Channel::Orderbooks {
                xch: Exchange::from(sc.exchange),
                pair: sc.pair.into(),
            },
            "trades" => Channel::Trades {
                xch: Exchange::from(sc.exchange),
                pair: sc.pair.into(),
            },
            "orders" => Channel::Orders {
                xch: Exchange::from(sc.exchange),
                pair: sc.pair.into(),
            },
            "candles" => Channel::Candles {
                xch: Exchange::from(sc.exchange),
                pair: sc.pair.into(),
            },
            _ => unimplemented!(),
        }
    }
}
