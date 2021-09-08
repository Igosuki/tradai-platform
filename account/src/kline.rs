use chrono::{DateTime, Utc};
use coinnect_rt::types::{Asset, Pair};
use std::time::Duration;

#[allow(dead_code)]
struct Kline {
    exchange: String,
    pair: Pair,
    asset: Asset,
    interval: Duration,
    candles: Vec<Candle>,
}

#[allow(dead_code)]
struct Candle {
    time: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}
