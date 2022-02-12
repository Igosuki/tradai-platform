use broker_core::error::Result;
use std::collections::HashMap;

pub(super) struct StandardOrder<'a> {
    pub type_order: &'a str,
    pub ordertype: &'a str,
    pub price: &'a str,
    pub price2: &'a str,
    pub volume: &'a str,
    pub leverage: &'a str,
    pub oflags: &'a str,
    pub starttm: &'a str,
    pub expiretm: &'a str,
    pub userref: &'a str,
    pub validate: &'a str,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub(super) struct OrderDescription {
    pub order: String,
    pub close: Option<String>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub(super) struct OrderResult {
    pub descr: OrderDescription,
    pub txids: Vec<String>,
}

#[derive(Deserialize)]
pub(super) struct RawOrderbook {
    pub asks: Vec<(String, String, i64)>,
    pub bids: Vec<(String, String, i64)>,
}

impl RawOrderbook {
    fn parse_line(line: &(String, String, i64)) -> Result<(f64, f64)> {
        Ok((line.0.parse::<f64>()?, line.1.parse::<f64>()?))
    }
    pub fn asks(&self) -> Result<Vec<(f64, f64)>> { self.asks.iter().map(Self::parse_line).collect() }
    pub fn bids(&self) -> Result<Vec<(f64, f64)>> { self.bids.iter().map(Self::parse_line).collect() }
}

pub(super) type Orderbooks = HashMap<String, RawOrderbook>;

#[derive(Deserialize)]
pub(super) struct TickerInfo {
    a: (String, String, String),
    b: (String, String, String),
    c: (String, String),
    v: (String, String),
}

impl TickerInfo {
    pub fn price(&self) -> Result<f64> { Ok(self.c.0.parse::<f64>()?) }
    pub fn ask(&self) -> Result<f64> { Ok(self.a.0.parse::<f64>()?) }
    pub fn bid(&self) -> Result<f64> { Ok(self.b.0.parse::<f64>()?) }
    pub fn volume(&self) -> Result<f64> { Ok(self.v.0.parse::<f64>()?) }
}
