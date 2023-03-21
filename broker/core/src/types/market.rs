use core::option::Option;
use core::option::Option::{None, Some};
use std::collections::BTreeMap;

use actix::Message;
use chrono::{DateTime, Duration, TimeZone, Utc};
use ordered_float::OrderedFloat;
use uuid::Uuid;

use crate::broker::{MarketEventEnvelopeRef, Subject};
use stats::kline::Resolution;
use util::ser::{decode_duration_opt, encode_duration_str_opt};
use util::time::now;

use crate::exchange::Exchange;
use crate::types::order::{Order, OrderEnforcement, OrderStatus, TradeType};
use crate::types::{Pair, Price, SecurityType, Symbol, Volume};

/// A market channel represents a unique stream of data that will be required to run a strategy
/// Historical and Real-Time data will be provided from this on a best effort basis.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, typed_builder::TypedBuilder)]
pub struct MarketChannel {
    /// A unique identifier for the security requested by this market data channel
    pub symbol: Symbol,
    /// The type of the ticker
    pub r#type: MarketChannelType,
    /// The minimal tick rate for the data, in reality max(tick_rate, exchange_tick_rate) will be used
    #[builder(default)]
    #[serde(deserialize_with = "decode_duration_opt", serialize_with = "encode_duration_str_opt")]
    pub tick_rate: Option<Duration>,
    /// If set, the data will be aggregated in OHLCV candles
    #[builder(default)]
    pub resolution: Option<Resolution>,
    /// Only send final candles
    #[builder(default)]
    pub only_final: Option<bool>,
    #[builder(default)]
    pub orderbook: Option<OrderbookConf>,
}

impl MarketChannel {
    pub fn exchange(&self) -> Exchange { self.symbol.xch }

    pub fn pair(&self) -> &Pair { &self.symbol.value }

    pub fn name(&self) -> &'static str {
        match self.r#type {
            MarketChannelType::Trades => "trades",
            MarketChannelType::Orderbooks { .. } => "order_books",
            MarketChannelType::OpenInterest => "interests",
            MarketChannelType::Candles => "candles",
            MarketChannelType::Quotes => "quotes",
            MarketChannelType::QuotesCandles => "book_candles",
        }
    }
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum MarketChannelType {
    /// Raw Trades see [MarketEvent::Trade]
    Trades,
    /// Order book changes see [MarketEvent::Orderbook]
    Orderbooks,
    /// Kline events see [MarketEvent::CandleTick]
    Candles,
    /// Open interest for futures see [MarketEvent::OpenInterest]
    OpenInterest,
    /// Layer 1 order book quotes see [MarketEvent::Quote]
    Quotes,
    /// Kline for layer 1 order book [MarketEvent::BookCandle]
    QuotesCandles,
}

impl From<&MarketEvent> for MarketChannelType {
    fn from(e: &MarketEvent) -> Self {
        match e {
            MarketEvent::Trade(_) => Self::Trades,
            MarketEvent::Orderbook(_) => Self::Orderbooks,
            MarketEvent::TradeCandle(_) => Self::Candles,
            MarketEvent::BookCandle(_) => Self::QuotesCandles,
        }
    }
}

impl From<MarketEvent> for MarketChannelType {
    fn from(e: MarketEvent) -> Self { From::from(&e) }
}

#[derive(Debug)]
pub struct LiveAggregatedOrderBook {
    pub depth: u16,
    pub pair: Pair,
    pub asks_by_price: BTreeMap<OrderedFloat<Price>, Offer>,
    pub bids_by_price: BTreeMap<OrderedFloat<Price>, Offer>,
    pub last_asks: Vec<Offer>,
    pub last_bids: Vec<Offer>,
    pub ts: i64,
    pub last_order_id: Option<String>,
}

impl LiveAggregatedOrderBook {
    pub fn default(pair: Pair) -> LiveAggregatedOrderBook {
        LiveAggregatedOrderBook {
            depth: DEFAULT_BOOK_DEPTH,
            pair,
            asks_by_price: BTreeMap::new(),
            bids_by_price: BTreeMap::new(),
            last_asks: vec![],
            last_bids: vec![],
            ts: 0,
            last_order_id: None,
        }
    }

    pub fn default_with_depth(pair: Pair, depth: Option<u16>) -> LiveAggregatedOrderBook {
        let mut book = LiveAggregatedOrderBook::default(pair);
        if let Some(depth) = depth {
            book.depth = depth;
        }
        book
    }

    pub fn order_book(&self) -> Orderbook {
        let asks: Vec<Offer> = self.asks_by_price.values().copied().take(self.depth as usize).collect();
        let bids: Vec<Offer> = self
            .bids_by_price
            .values()
            .copied()
            .rev()
            .take(self.depth as usize)
            .collect();
        //        debug!("Latest order book highest ask {:?}", latest_order_book.asks.get(0).map(|(k, v)| (k.as_f32().unwrap(), v.as_f32().unwrap())));
        Orderbook {
            timestamp: Utc::now().timestamp_millis(),
            pair: self.pair.clone(),
            asks,
            bids,
            last_order_id: None,
        }
    }

    pub fn latest_order_book(&mut self) -> Option<Orderbook> {
        let latest_order_book: Orderbook = self.order_book();
        if latest_order_book.asks == self.last_asks && latest_order_book.bids == self.last_bids {
            trace!("Order book top unchanged, not flushing");
            None
        } else {
            self.last_asks = latest_order_book.asks.clone();
            self.last_bids = latest_order_book.bids.clone();
            Some(latest_order_book)
        }
    }

    pub fn reset_asks<'a, I>(&mut self, iter: I)
    where
        I: Iterator<Item = &'a Offer>,
    {
        self.asks_by_price = BTreeMap::new();
        for kp in iter {
            self.asks_by_price.entry(kp.0.into()).or_insert(*kp);
        }
    }

    pub fn reset_bids<'a, I>(&mut self, iter: I)
    where
        I: Iterator<Item = &'a Offer>,
    {
        self.bids_by_price = BTreeMap::new();
        for kp in iter {
            self.bids_by_price.entry(kp.0.into()).or_insert(*kp);
        }
    }

    pub fn reset_asks_n<I>(&mut self, iter: I)
    where
        I: Iterator<Item = Offer>,
    {
        self.asks_by_price = BTreeMap::new();
        for kp in iter {
            self.asks_by_price.entry(kp.0.into()).or_insert(kp);
        }
    }

    pub fn reset_bids_n<I>(&mut self, iter: I)
    where
        I: Iterator<Item = Offer>,
    {
        self.bids_by_price = BTreeMap::new();
        for kp in iter {
            self.bids_by_price.entry(kp.0.into()).or_insert(kp);
        }
    }

    pub fn update_asks<I>(&mut self, iter: I)
    where
        I: Iterator<Item = Offer>,
    {
        iter.for_each(|kp| self.update_ask(kp));
    }

    pub fn update_ask(&mut self, kp: Offer) {
        let asks = &mut self.asks_by_price;
        Self::upsert(asks, kp);
    }

    pub fn update_bids<I>(&mut self, iter: I)
    where
        I: Iterator<Item = Offer>,
    {
        iter.for_each(|kp| self.update_bid(kp));
    }

    pub fn update_bid(&mut self, kp: Offer) {
        let bids = &mut self.bids_by_price;
        Self::upsert(bids, kp);
    }

    pub fn set_ts(&mut self, i: i64) { self.ts = i; }

    #[allow(dead_code)]
    pub fn set_last_order_id(&mut self, last_order_id: Option<String>) { self.last_order_id = last_order_id }

    fn upsert(m: &mut BTreeMap<OrderedFloat<Price>, Offer>, kp: Offer) {
        if kp.1 == 0.0 {
            m.remove(&kp.0.into());
        } else {
            m.insert(kp.0.into(), kp);
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Trade {
    /// UNIX timestamp in ms (when the event occured)
    pub event_ms: i64,
    /// The Pair corresponding to the Ticker returned (maybe useful later for asynchronous APIs)
    pub pair: Pair,
    /// Amount of the trade
    pub amount: f64,
    /// Price of the trade
    pub price: Price,
    /// Buy or Sell
    pub tt: TradeType,
}

#[allow(clippy::large_enum_variant)]
#[derive(Message, Clone, Debug, Deserialize, Serialize, PartialEq)]
#[rtype(result = "()")]
#[serde(tag = "type")]
pub enum MarketEvent {
    Trade(Trade),
    Orderbook(Orderbook),
    TradeCandle(Candle),
    BookCandle(BookCandle),
}

impl MarketEvent {
    pub fn chan(&self) -> &'static str {
        match self {
            MarketEvent::Trade(_) => "trades",
            MarketEvent::Orderbook(_) => "order_book",
            MarketEvent::TradeCandle(_) => "trade_candles",
            MarketEvent::BookCandle(_) => "book_candles",
        }
    }

    /// TODO: remove, as the enveloppe should have the pair already
    pub fn pair(&self) -> Pair {
        match self {
            Self::Trade(ref e) => e.pair.clone(),
            Self::Orderbook(ref e) => e.pair.clone(),
            Self::TradeCandle(ref e) => e.pair.clone(),
            Self::BookCandle(ref e) => e.pair.clone(),
        }
    }

    pub fn time(&self) -> DateTime<Utc> {
        match self {
            MarketEvent::Trade(t) => Utc.timestamp_millis_opt(t.event_ms).unwrap(),
            MarketEvent::Orderbook(ob) => Utc.timestamp_millis_opt(ob.timestamp).unwrap(),
            MarketEvent::TradeCandle(c) => c.event_time,
            MarketEvent::BookCandle(c) => c.event_time,
        }
    }

    /// Volume Weighted Average Price
    pub fn vwap(&self) -> f64 {
        match self {
            MarketEvent::Trade(t) => t.price,
            MarketEvent::Orderbook(o) => o.vwap().unwrap_or(0.0),
            // TODO: vwap should be made available in candles
            MarketEvent::TradeCandle(ct) => (ct.high + ct.low) / 2.0,
            MarketEvent::BookCandle(bc) => bc.mid.close,
        }
    }

    pub fn high(&self) -> f64 {
        match self {
            MarketEvent::Trade(t) => t.price,
            MarketEvent::Orderbook(o) => o.top_bid().map_or(0.0, |b| b.0),
            MarketEvent::TradeCandle(ct) => ct.high,
            MarketEvent::BookCandle(bc) => bc.ask.high,
        }
    }

    pub fn low(&self) -> f64 {
        match self {
            MarketEvent::Trade(t) => t.price,
            MarketEvent::Orderbook(o) => o.top_ask().map_or(0.0, |b| b.0),
            MarketEvent::TradeCandle(ct) => ct.low,
            MarketEvent::BookCandle(bc) => bc.ask.low,
        }
    }

    pub fn close(&self) -> f64 {
        match self {
            MarketEvent::Trade(t) => t.price,
            MarketEvent::Orderbook(o) => o.top_bid().map_or(0.0, |b| b.0),
            MarketEvent::TradeCandle(ct) => ct.close,
            MarketEvent::BookCandle(bc) => bc.ask.close,
        }
    }

    pub fn open(&self) -> f64 {
        match self {
            MarketEvent::Trade(t) => t.price,
            MarketEvent::Orderbook(o) => o.top_ask().map_or(0.0, |b| b.0),
            MarketEvent::TradeCandle(ct) => ct.open,
            MarketEvent::BookCandle(bc) => bc.ask.open,
        }
    }

    pub fn vol(&self) -> f64 {
        match self {
            MarketEvent::Trade(t) => t.amount,
            MarketEvent::Orderbook(o) => o.vol(),
            MarketEvent::TradeCandle(ct) => ct.quote_volume,
            MarketEvent::BookCandle(bc) => bc.ask.quote_volume,
        }
    }

    pub fn price(&self) -> f64 {
        match self {
            MarketEvent::Trade(t) => t.price,
            MarketEvent::Orderbook(o) => o.top_ask().or_else(|| o.top_bid()).unwrap_or((0.0, 0.0)).0,
            MarketEvent::TradeCandle(t) => t.close,
            MarketEvent::BookCandle(bc) => bc.ask.close,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type")]
pub struct MarketEventEnvelope {
    pub symbol: Symbol,
    pub trace_id: Uuid,
    pub ts: DateTime<Utc>,
    pub e: MarketEvent,
    pub sec_type: SecurityType,
}

impl MarketEventEnvelope {
    pub fn new(symbol: Symbol, e: MarketEvent) -> Self {
        Self {
            symbol,
            e,
            trace_id: Uuid::new_v4(),
            ts: now(),
            sec_type: SecurityType::Crypto,
        }
    }

    pub fn order_book_event(
        symbol: Symbol,
        ts: i64,
        asks: Vec<(f64, f64)>,
        bids: Vec<(f64, f64)>,
    ) -> MarketEventEnvelope {
        let orderbook = Orderbook {
            timestamp: ts,
            pair: symbol.value.clone(),
            asks,
            bids,
            last_order_id: None,
        };
        Self::new(symbol, MarketEvent::Orderbook(orderbook))
    }

    pub fn trade_event(symbol: Symbol, ts: i64, price: f64, qty: f64, tt: TradeType) -> MarketEventEnvelope {
        let trade = Trade {
            event_ms: ts,
            pair: symbol.value.clone(),
            amount: qty,
            price,
            tt,
        };
        Self::new(symbol, MarketEvent::Trade(trade))
    }
}

impl Message for MarketEventEnvelope {
    type Result = std::result::Result<(), anyhow::Error>;
}

#[derive(Debug)]
pub struct Ticker {
    /// UNIX timestamp in ms (when the response was received)
    pub timestamp: i64,
    /// The Pair corresponding to the Ticker returned (maybe useful later for asynchronous APIs)
    pub pair: Pair,
    /// Last trade price found in the history
    pub last_trade_price: Price,
    /// Lowest ask price found in Orderbook
    pub lowest_ask: Price,
    /// Highest bid price found in Orderbook
    pub highest_bid: Price,
    /// Last 24 hours volume (quote-volume)
    pub volume: Option<Volume>,
}

const DEFAULT_BOOK_DEPTH: u16 = 5;

impl Default for OrderUpdate {
    fn default() -> Self {
        OrderUpdate {
            enforcement: OrderEnforcement::GTC,
            side: TradeType::Sell,
            orig_order_id: None,
            order_id: 0,
            symbol: "".to_string(),
            timestamp: 0,
            new_status: OrderStatus::New,
            orig_status: OrderStatus::New,
            is_on_the_book: false,
            qty: 0.0,
            quote_qty: 0.0,
            price: 0.0,
            stop_price: 0.0,
            iceberg_qty: 0.0,
            commission: 0.0,
            commission_asset: None,
            last_executed_qty: 0.0,
            cummulative_filled_qty: 0.0,
            last_executed_price: 0.0,
            cummulative_quote_asset_transacted_qty: 0.0,
            last_quote_asset_transacted_qty: 0.0,
            quote_order_qty: 0.0,
            rejection_reason: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BalanceUpdate {
    /// Event time occurrence
    pub event_time: DateTime<Utc>,

    /// Server time occurrence
    pub server_time: DateTime<Utc>,

    /// Asset symbol concerned with this balance update
    pub symbol: String,

    /// Amount to add or substract to the balance
    pub delta: f64,

    /// The time at which the update was resolved
    pub clear_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct OrderUpdate {
    pub enforcement: OrderEnforcement,
    pub side: TradeType,
    pub orig_order_id: Option<String>,
    pub order_id: u64,
    pub symbol: String,
    pub timestamp: u64,
    pub new_status: OrderStatus,
    pub orig_status: OrderStatus,
    pub is_on_the_book: bool,
    pub qty: f64,
    pub quote_qty: f64,
    pub price: f64,
    pub stop_price: f64,
    pub iceberg_qty: f64,
    pub commission: f64,
    pub commission_asset: Option<String>,
    pub last_executed_qty: f64,
    pub cummulative_filled_qty: f64,
    pub last_executed_price: f64,
    pub cummulative_quote_asset_transacted_qty: f64,
    pub last_quote_asset_transacted_qty: f64,
    pub quote_order_qty: f64,
    pub rejection_reason: Option<String>,
}

impl From<Order> for OrderUpdate {
    fn from(o: Order) -> Self {
        OrderUpdate {
            enforcement: o.enforcement,
            side: o.side,
            orig_order_id: Some(o.orig_order_id),
            order_id: o.order_id.parse::<u64>().unwrap_or(0),
            symbol: o.symbol.to_string(),
            timestamp: o.last_event_time,
            new_status: o.status.clone(),
            orig_status: o.status.clone(),
            is_on_the_book: o.is_in_transaction,
            qty: o.executed_qty,
            quote_qty: o.orig_qty,
            price: o.price,
            stop_price: o.stop_price,
            iceberg_qty: o.iceberg_qty,
            commission: 0.0,
            commission_asset: None,
            last_executed_qty: o.executed_qty,
            cummulative_filled_qty: o.cumulative_quote_qty,
            last_executed_price: o.price,
            cummulative_quote_asset_transacted_qty: 0.0,
            last_quote_asset_transacted_qty: 0.0,
            quote_order_qty: o.orig_quote_order_qty,
            rejection_reason: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct OrderbookConf {
    pub depth: Option<u16>,
    pub level: OrderbookLevel,
}

impl Default for OrderbookConf {
    fn default() -> Self {
        Self {
            depth: Some(5),
            level: OrderbookLevel::Level2,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum OrderbookLevel {
    /// Only top quote and bid
    Level1,
    /// Orderbook at arbitrary depth
    Level2,
    /// Complete orderbook
    Level3,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Orderbook {
    /// UNIX timestamp in ms (when the response was received)
    pub timestamp: i64,
    /// The Pair corresponding to the Orderbook returned (maybe useful later for asynchronous APIs)
    pub pair: Pair,
    /// Vec containing the ask offers (by ascending price)
    pub asks: Vec<Offer>,
    /// Vec containing the bid offers (by descending price)
    pub bids: Vec<Offer>,
    /// Id of the most recent order in the book
    pub last_order_id: Option<String>,
}

impl Orderbook {
    /// Convenient function that returns the average price from the orderbook
    /// Return None if Orderbook is empty
    /// `Average price = (lowest ask + highest bid)/2`
    pub fn avg_price(&self) -> Option<Price> {
        if self.asks.is_empty() || self.bids.is_empty() {
            return None;
        }
        Some((self.asks[0].0 + self.bids[0].0) / 2.0)
    }

    /// Convenient function that returns the value weighted average price
    /// Return None if Orderbook is empty
    pub fn vwap(&self) -> Option<Price> {
        if self.asks.is_empty() || self.bids.is_empty() {
            return None;
        }
        let vwap = (self.asks.iter().map(|a| a.0 * a.1).sum::<f64>()
            + self.bids.iter().map(|b| b.0 * b.1).sum::<f64>())
            / self.vol();
        Some(vwap)
    }

    pub fn top_ask(&self) -> Option<Offer> {
        if self.asks.is_empty() {
            None
        } else {
            Some(self.asks[0])
        }
    }

    pub fn top_bid(&self) -> Option<Offer> {
        if self.bids.is_empty() {
            None
        } else {
            Some(self.bids[0])
        }
    }

    pub fn vol(&self) -> f64 { self.bids.iter().map(|v| v.1).sum::<f64>() + self.asks.iter().map(|v| v.1).sum::<f64>() }

    pub fn has_bids_and_asks(&self) -> bool { !self.asks.is_empty() && !self.bids.is_empty() }
}

pub type Offer = (Price, Volume);

/// Normalised OHLCV data from an [Interval] with the associated [DateTime] UTC timestamp;
#[derive(Debug, Deserialize, Serialize, PartialOrd, PartialEq, Clone)]
pub struct Candle {
    /// The time this candle was generated at
    pub event_time: DateTime<Utc>,
    /// Market pair
    pub pair: Pair,
    /// The start time of this candle
    pub start_time: DateTime<Utc>,
    /// The close time of this candle
    pub end_time: DateTime<Utc>,
    /// Open price
    pub open: f64,
    /// Highest price within the interval
    pub high: f64,
    /// Lowest price within the interval
    pub low: f64,
    /// Close price
    pub close: f64,
    /// Traded volume in base asset
    pub volume: f64,
    /// Traded volume in quote asset
    pub quote_volume: f64,
    /// Trades count
    pub trade_count: u64,
    /// If the candle is closed
    pub is_final: bool,
}

/// Normalised OHLCV data from an [Interval] with the associated [DateTime] UTC timestamp;
#[derive(Debug, Deserialize, Serialize, PartialOrd, PartialEq, Clone)]
pub struct BookCandle {
    pub bid: Candle,
    pub ask: Candle,
    pub mid: Candle,
    pub is_final: bool,
    /// Market pair
    pub pair: Pair,
    /// The time this candle was generated at
    pub event_time: DateTime<Utc>,
}

pub struct BookTick {
    pub ask: f64,
    pub askq: f64,
    pub bid: f64,
    pub bidq: f64,
    pub mid: f64,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MarketChannelTopic(pub Symbol, pub MarketChannelType);

impl From<MarketEventEnvelope> for MarketChannelTopic {
    fn from(e: MarketEventEnvelope) -> Self { MarketChannelTopic(e.symbol, e.e.into()) }
}

impl From<&MarketEventEnvelope> for MarketChannelTopic {
    fn from(e: &MarketEventEnvelope) -> Self { MarketChannelTopic(e.symbol.clone(), (&e.e).into()) }
}

impl From<MarketEventEnvelopeRef> for MarketChannelTopic {
    fn from(e: MarketEventEnvelopeRef) -> Self { (e.as_ref()).into() }
}

impl Subject<MarketEventEnvelope> for MarketChannelTopic {}

impl Subject<MarketEventEnvelopeRef> for MarketChannelTopic {}

impl From<MarketChannel> for MarketChannelTopic {
    fn from(mc: MarketChannel) -> Self { Self(mc.symbol, mc.r#type) }
}

impl From<&MarketChannel> for MarketChannelTopic {
    fn from(mc: &MarketChannel) -> Self { Self(mc.symbol.clone(), mc.r#type) }
}
