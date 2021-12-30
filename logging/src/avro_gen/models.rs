#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, serde::Deserialize, serde::Serialize)]
pub enum TradeType {
    #[serde(rename = "BUY")]
    Buy,
    #[serde(rename = "SELL")]
    Sell,
}

lazy_static! {
    pub static ref ORDERBOOK_SCHEMA : avro_rs::schema::Schema = avro_rs::schema::Schema::parse_str("{\"type\":\"record\",\"name\":\"Orderbook\",\"fields\":[{\"name\":\"event_ms\",\"type\":\"long\"},{\"name\":\"pair\",\"type\":\"string\"},{\"name\":\"asks\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"double\"}}},{\"name\":\"bids\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"double\"}}}]}").unwrap();
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct Orderbook {
    pub event_ms: i64,
    pub pair: String,
    pub asks: Vec<Vec<f64>>,
    pub bids: Vec<Vec<f64>>,
}

#[allow(clippy::derivable_impls)]
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

lazy_static! {
    pub static ref LIVEORDER_SCHEMA : avro_rs::schema::Schema = avro_rs::schema::Schema::parse_str("{\"type\":\"record\",\"name\":\"LiveOrder\",\"fields\":[{\"name\":\"event_ms\",\"type\":\"long\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"pair\",\"type\":\"string\"},{\"name\":\"price\",\"type\":\"double\"},{\"name\":\"tt\",\"type\":\"int\"}]}").unwrap();
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct LiveOrder {
    pub event_ms: i64,
    pub amount: f64,
    pub pair: String,
    pub price: f64,
    pub tt: i32,
}

#[allow(clippy::derivable_impls)]
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
    pub static ref LIVETRADE_SCHEMA : avro_rs::schema::Schema = avro_rs::schema::Schema::parse_str("{\"type\":\"record\",\"name\":\"LiveTrade\",\"fields\":[{\"name\":\"event_ms\",\"type\":\"long\"},{\"name\":\"pair\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"price\",\"type\":\"double\"},{\"name\":\"tt\",\"type\":\"int\"}]}").unwrap();
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct LiveTrade {
    pub event_ms: i64,
    pub pair: String,
    pub amount: f64,
    pub price: f64,
    pub tt: i32,
}

#[allow(clippy::derivable_impls)]
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
    pub static ref CANDLE_SCHEMA : avro_rs::schema::Schema = avro_rs::schema::Schema::parse_str("{\"type\":\"record\",\"name\":\"Candle\",\"fields\":[{\"name\":\"event_ms\",\"type\":\"long\"},{\"name\":\"pair\",\"type\":\"string\"},{\"name\":\"start_ms\",\"type\":\"long\"},{\"name\":\"end_ms\",\"type\":\"long\"},{\"name\":\"open\",\"type\":\"double\"},{\"name\":\"high\",\"type\":\"double\"},{\"name\":\"low\",\"type\":\"double\"},{\"name\":\"close\",\"type\":\"double\"},{\"name\":\"volume\",\"type\":\"double\"},{\"name\":\"quote_volume\",\"type\":\"double\"},{\"name\":\"trade_count\",\"type\":\"long\"}]}").unwrap();
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct Candle {
    pub event_ms: i64,
    pub pair: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub trade_count: i64,
}

#[allow(clippy::derivable_impls)]
impl Default for Candle {
    fn default() -> Candle {
        Candle {
            event_ms: 0,
            pair: String::default(),
            start_ms: 0,
            end_ms: 0,
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
            quote_volume: 0.0,
            trade_count: 0,
        }
    }
}
