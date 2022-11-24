use serde::{Deserialize, Serialize};
use serde_aux::prelude::*;

use broker_core::types::{MarketEvent, MarketSymbol, Orderbook, Pair, Ticker as CoinnectTicker, Trade};

use super::utils;

#[derive(Deserialize)]
pub struct Ticker {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub ask: f64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub bid: f64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub high: f64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub last: f64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub low: f64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub open: f64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub timestamp: i64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub volume: f64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub vwap: f64,
}

impl From<Ticker> for CoinnectTicker {
    fn from(t: Ticker) -> Self {
        Self {
            timestamp: t.timestamp,
            pair: Pair::default(),
            last_trade_price: t.last,
            lowest_ask: t.low,
            highest_bid: t.high,
            volume: Some(t.volume),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LiveTrade {
    microtimestamp: String,
    amount: f64,
    buy_order_id: i64,
    sell_order_id: i64,
    amount_str: String,
    price_str: String,
    timestamp: String,
    price: f64,
    #[serde(rename(serialize = "type", deserialize = "type"))]
    ty: i64,
    id: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LiveOrderBook {
    microtimestamp: String,
    timestamp: String,
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
}

impl From<LiveOrderBook> for Orderbook {
    fn from(live_book: LiveOrderBook) -> Self {
        Self {
            asks: live_book
                .asks
                .iter()
                .map(|(p, v)| (p.parse::<f64>().unwrap(), v.parse::<f64>().unwrap()))
                .collect(),
            bids: live_book
                .bids
                .iter()
                .map(|(p, v)| (p.parse::<f64>().unwrap(), v.parse::<f64>().unwrap()))
                .collect(),
            timestamp: live_book.microtimestamp.parse::<i64>().unwrap(),
            pair: Pair::default(),
            last_order_id: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LiveOrder {
    id: i64,
    amount: f32,
    amount_str: String,
    price: f32,
    price_str: String,
    order_type: i64,
    datetime: String,
    microtimestamp: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Enveloppe<T> {
    data: T,
    pub channel: String,
}

pub type PlainEvent = Enveloppe<serde_json::Value>;

#[derive(Debug, Serialize, Deserialize, Message)]
#[serde(tag = "event")]
#[rtype(result = "()")]
pub enum Event {
    #[serde(alias = "bts:subscription_succeeded")]
    SubSucceeded(PlainEvent),
    #[serde(alias = "bts:request_reconnect")]
    ReconnectRequest(PlainEvent),
    #[serde(alias = "trade")]
    LiveTrade(Enveloppe<LiveTrade>),
    #[serde(alias = "data")]
    LiveFullOrderBook(Enveloppe<LiveOrderBook>),
    #[serde(alias = "order_created", alias = "order_deleted")]
    LiveOrder(Enveloppe<LiveOrder>),
}

// TODO: the pair is unknown in events, this is a problem.
impl TryFrom<Event> for MarketEvent {
    type Error = ();

    fn try_from(event: Event) -> Result<Self, Self::Error> {
        match event {
            Event::LiveTrade(e) => Ok(MarketEvent::Trade(Trade {
                amount: e.data.amount,
                event_ms: e.data.microtimestamp.parse::<i64>().unwrap(),
                price: e.data.price,
                tt: e.data.ty.into(),
                pair: (*utils::get_pair_string(&Pair::from("")).unwrap()).into(),
            })),
            Event::LiveFullOrderBook(e) => Ok(MarketEvent::Orderbook(e.data.into())),
            _ => Err(()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Data {
    channel: String,
}

#[derive(Serialize, Deserialize, Debug, Message)]
#[rtype(result = "()")]
pub struct Subscription {
    event: String,
    data: Data,
}

pub fn subscription(c: StreamChannel, currency_pair: &str) -> Subscription {
    let channel_str = match c {
        StreamChannel::Trades => "live_trades",
        StreamChannel::Orders => "live_orders",
        StreamChannel::PlainOrderbook => "order_book",
        StreamChannel::DetailedOrderbook => "detail_order_book",
        StreamChannel::DiffOrderbook => "diff_order_book",
    };
    Subscription {
        event: String::from("bts:subscribe"),
        data: Data {
            channel: format!("{}_{}", channel_str, currency_pair),
        },
    }
}

#[non_exhaustive]
pub(crate) struct PublicQuery {
    pub method: &'static str,
    pub pair: MarketSymbol,
}

#[derive(Deserialize)]
pub(crate) struct Transaction {
    pub date: String,
    pub amount: String,
    #[allow(dead_code)]
    pub tid: String,
    #[serde(rename = "type")]
    pub type_r: String,
    pub price: String,
}

#[cfg(test)]
mod model_tests {
    use super::*;

    #[tokio::test]
    async fn deserialize_live_trade() {
        let _v: Event = serde_json::from_slice(b"{\"data\": {\"microtimestamp\": \"1577146143220559\", \"amount\": 0.00434678, \"buy_order_id\": 4481152330, \"sell_order_id\": 4481152280, \"amount_str\": \"0.00434678\", \"price_str\": \"7312.91\", \"timestamp\": \"1577146143\", \"price\": 7312.91, \"type\": 0, \"id\": 102177815}, \"event\": \"trade\", \"channel\": \"live_trades_btcusd\"}").unwrap();
    }

    #[tokio::test]
    async fn deserialize_sub_succeeded() {
        let _v: Event = serde_json::from_slice(b"{\"data\": {\"microtimestamp\": \"1577146143220559\", \"amount\": 0.00434678, \"buy_order_id\": 4481152330, \"sell_order_id\": 4481152280, \"amount_str\": \"0.00434678\", \"price_str\": \"7312.91\", \"timestamp\": \"1577146143\", \"price\": 7312.91, \"type\": 0, \"id\": 102177815}, \"event\": \"trade\", \"channel\": \"live_trades_btcusd\"}").unwrap();
    }
}
