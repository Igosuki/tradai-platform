use serde::{Deserialize, Serialize};
use lazy_static;
use avro_rs::schema::Schema;

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Deserialize, Serialize)]
pub enum TradeType {
    #[serde(rename = "BUY")]
    Buy,
    #[serde(rename = "SELL")]
    Sell,
}


lazy_static! {
    pub static ref LIVEORDER_SCHEMA : Schema = Schema::parse_str("{\"type\":\"record\",\"name\":\"LiveOrder\",\"fields\":[{\"name\":\"event_ms\",\"type\":\"long\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"pair\",\"type\":\"string\"},{\"name\":\"price\",\"type\":\"double\"},{\"name\":\"tt\",\"type\":\"int\"}]}").unwrap();
}

#[serde(default)]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct LiveOrder {
    pub event_ms: i64,
    pub amount: f64,
    pub pair: String,
    pub price: f64,
    pub tt: i32,
}

impl Default for LiveOrder {
    fn default() -> LiveOrder {
        LiveOrder {
            event_ms: 0,
            amount: 0.0,
            pair: String::default(),
            price: 0.0,
            tt: 0,
        }
    }
}


lazy_static! {
    pub static ref LIVETRADE_SCHEMA : Schema = Schema::parse_str("{\"type\":\"record\",\"name\":\"LiveTrade\",\"fields\":[{\"name\":\"event_ms\",\"type\":\"long\"},{\"name\":\"pair\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"price\",\"type\":\"double\"},{\"name\":\"tt\",\"type\":\"int\"}]}").unwrap();
}

#[serde(default)]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct LiveTrade {
    pub event_ms: i64,
    pub pair: String,
    pub amount: f64,
    pub price: f64,
    pub tt: i32,
}

impl Default for LiveTrade {
    fn default() -> LiveTrade {
        LiveTrade {
            event_ms: 0,
            pair: String::default(),
            amount: 0.0,
            price: 0.0,
            tt: 0,
        }
    }
}


lazy_static! {
    pub static ref ORDERBOOK_SCHEMA : Schema = Schema::parse_str("{\"type\":\"record\",\"name\":\"Orderbook\",\"fields\":[{\"name\":\"event_ms\",\"type\":\"long\"},{\"name\":\"pair\",\"type\":\"string\"},{\"name\":\"asks\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"double\"}}},{\"name\":\"bids\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"double\"}}}]}").unwrap();
}

#[serde(default)]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct Orderbook {
    pub event_ms: i64,
    pub pair: String,
    pub asks: Vec<Vec<f64>>,
    pub bids: Vec<Vec<f64>>,
}

impl Default for Orderbook {
    fn default() -> Orderbook {
        Orderbook {
            event_ms: 0,
            pair: String::default(),
            asks: vec![],
            bids: vec![],
        }
    }
}
