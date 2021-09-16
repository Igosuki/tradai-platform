use std::convert::TryFrom;

use chrono::{DateTime, TimeZone, Utc};
use itertools::Itertools;
use log::Level::Debug;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use strum_macros::{AsRefStr, EnumString};

use coinnect_rt::exchange::Exchange;
use coinnect_rt::types::OrderStatus as ExchangeOrderStatus;
use coinnect_rt::types::{AddOrderRequest, AssetType, MarginSideEffect, OrderEnforcement, OrderSubmission, OrderType,
                         OrderUpdate, Orderbook, TradeType};

use crate::error::DataTableError;
use crate::order_types::Rejection;

// ------------ Behavioral Types ---------

#[derive(Copy, Clone, PartialEq, Eq, Debug, Deserialize, Serialize, juniper::GraphQLEnum)]
#[serde(rename_all = "snake_case")]
pub enum OrderMode {
    Market,
    Limit,
}

impl Default for OrderMode {
    fn default() -> Self { Self::Limit }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, EnumString)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionInstruction {
    CancelIfNotBest,
    DoNotIncrease,
    DoNotReduce,
    LastPrice,
}

// --------- Event Types ---------

#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StopEvent {
    Gain,
    Loss,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TradeOperation {
    pub kind: TradeKind,
    pub pair: String,
    pub qty: f64,
    pub price: f64,
    pub dry_mode: bool,
    #[serde(default)]
    pub mode: OrderMode,
    #[serde(default)]
    pub asset_type: AssetType,
    pub margin_interest_rate: Option<f64>,
}

#[juniper::graphql_object]
impl TradeOperation {
    fn kind(&self) -> &TradeKind { &self.kind }
    fn pair(&self) -> &str { &self.pair }
    fn qty(&self) -> f64 { self.qty }
    fn price(&self) -> f64 { self.price }
    fn dry_mode(&self) -> bool { self.dry_mode }
    fn mode(&self) -> OrderMode { self.mode }
    fn asset_type(&self) -> &str { self.asset_type.as_ref() }
}

impl TradeOperation {
    pub fn with_new_price(&self, new_price: f64) -> TradeOperation {
        TradeOperation {
            price: new_price,
            ..self.clone()
        }
    }

    pub fn to_request(&self, mode: &OrderMode, asset_type: &AssetType) -> AddOrderRequest {
        let mut request: AddOrderRequest = self.clone().into();
        match mode {
            OrderMode::Limit => {
                request.order_type = OrderType::Limit;
                request.enforcement = Some(OrderEnforcement::FOK);
            }
            OrderMode::Market => {
                request.order_type = OrderType::Market;
                request.price = None;
            }
        }
        request.asset_type = Some(*asset_type);
        request
    }
}

impl From<TradeOperation> for AddOrderRequest {
    fn from(to: TradeOperation) -> Self {
        AddOrderRequest {
            pair: to.pair.into(),
            side: to.kind.into(),
            quantity: Some(to.qty),
            price: Some(to.price),
            dry_run: to.dry_mode,
            ..AddOrderRequest::default()
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OperationEvent {
    pub(crate) op: OperationKind,
    pub(crate) pos: PositionKind,
    pub(crate) at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TradeEvent {
    pub(crate) op: TradeKind,
    pub(crate) qty: f64,
    pub(crate) pair: String,
    pub(crate) price: f64,
    pub(crate) strat_value: f64,
    pub(crate) at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "event")]
pub enum StratEvent {
    Stop { stop: StopEvent },
    Operation(OperationEvent),
    Trade(TradeEvent),
}

impl StratEvent {
    pub fn log(&self) {
        if log_enabled!(Debug) {
            tracing::debug!(strat_event = ?self);
        }
    }
}

impl From<StopEvent> for StratEvent {
    fn from(stop: StopEvent) -> Self { Self::Stop { stop } }
}

// --------- Data Types ---------

#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeKind {
    #[strum(serialize = "buy")]
    Buy,
    #[strum(serialize = "sell")]
    Sell,
}

impl From<TradeKind> for TradeType {
    fn from(tk: TradeKind) -> TradeType {
        match tk {
            TradeKind::Buy => TradeType::Buy,
            TradeKind::Sell => TradeType::Sell,
        }
    }
}

impl From<TradeKind> for i32 {
    fn from(tk: TradeKind) -> i32 {
        match tk {
            TradeKind::Buy => 0,
            TradeKind::Sell => 1,
        }
    }
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum)]
#[serde(rename_all = "lowercase")]
pub enum PositionKind {
    #[strum(serialize = "short")]
    Short,
    #[strum(serialize = "long")]
    Long,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationKind {
    #[strum(serialize = "open")]
    Open,
    #[strum(serialize = "close")]
    Close,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "python", pyclass)]
pub struct BookPosition {
    pub mid: f64,
    // mid = (top_ask + top_bid) / 2, alias: crypto1_m
    pub ask: f64,
    // crypto_a
    pub ask_q: f64,
    // crypto_a_q
    pub bid: f64,
    // crypto_b
    pub bid_q: f64,
    // crypto_b_q
    pub event_time: DateTime<Utc>,
}

impl BookPosition {
    pub fn new(event_time: DateTime<Utc>, asks: &[(f64, f64)], bids: &[(f64, f64)]) -> Self {
        let first_ask = asks[0];
        let first_bid = bids[0];
        Self {
            ask: first_ask.0,
            ask_q: first_ask.1,
            bid: first_bid.0,
            bid_q: first_bid.1,
            mid: Self::mid(asks, bids),
            event_time,
        }
    }

    fn mid(asks: &[(f64, f64)], bids: &[(f64, f64)]) -> f64 {
        let (asks_iter, asks_iter2) = asks.iter().tee();
        let (bids_iter, bids_iter2) = bids.iter().tee();
        (asks_iter.map(|a| a.0 * a.1).sum::<f64>() + bids_iter.map(|b| b.0 * b.1).sum::<f64>())
            / asks_iter2.interleave(bids_iter2).map(|t| t.1).sum::<f64>()
    }
}

impl TryFrom<Orderbook> for BookPosition {
    type Error = DataTableError;

    fn try_from(t: Orderbook) -> Result<Self, Self::Error> {
        if t.asks.is_empty() {
            return Err(DataTableError::MissingAsks);
        }
        if t.bids.is_empty() {
            return Err(DataTableError::MissingBids);
        }
        let event_time = Utc.timestamp_millis(t.timestamp);
        Ok(BookPosition::new(event_time, &t.asks, &t.bids))
    }
}

impl<'a> TryFrom<&'a Orderbook> for BookPosition {
    type Error = DataTableError;

    fn try_from(t: &'a Orderbook) -> Result<Self, Self::Error> {
        if t.asks.is_empty() {
            return Err(DataTableError::MissingAsks);
        }
        if t.bids.is_empty() {
            return Err(DataTableError::MissingBids);
        }
        let event_time = Utc.timestamp_millis(t.timestamp);
        Ok(BookPosition::new(event_time, &t.asks, &t.bids))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Staged,
    Created,
    Filled,
    PartiallyFilled,
    Rejected,
    Canceled,
}

impl From<ExchangeOrderStatus> for OrderStatus {
    fn from(o: ExchangeOrderStatus) -> Self {
        match o {
            ExchangeOrderStatus::New => Self::Created,
            ExchangeOrderStatus::PartialyFilled => Self::PartiallyFilled,
            ExchangeOrderStatus::Filled => Self::Filled,
            ExchangeOrderStatus::Canceled => Self::Canceled,
            ExchangeOrderStatus::PendingCancel => Self::Canceled,
            ExchangeOrderStatus::Rejected => Self::Rejected,
            ExchangeOrderStatus::Expired => Self::Rejected,
            ExchangeOrderStatus::Traded => Self::Filled,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct OrderDetail {
    /// Order id
    id: String,
    /// Optional transaction id, if this order was part of a larger transaction
    transaction_id: Option<String>,
    /// Identifer with the remote platform
    remote_id: Option<String>,
    status: OrderStatus,
    exchange: String,
    pair: String,
    base_asset: String,
    quote_asset: String,
    side: TradeType,
    order_type: OrderType,
    enforcement: Option<OrderEnforcement>,
    base_qty: Option<f64>,
    quote_qty: Option<f64>,
    price: Option<f64>,
    stop_price: Option<f64>,
    iceberg_qty: Option<f64>,
    is_test: bool,
    asset_type: AssetType,
    executed_qty: Option<f64>,
    cummulative_quote_qty: Option<f64>,
    margin_side_effect: Option<MarginSideEffect>,
    borrowed_amount: Option<f64>,
    borrowed_asset: Option<String>,
    fills: Vec<OrderFill>,
    /// Weighted price updated from fills
    weighted_price: f64,
    total_executed_qty: f64,
    rejection_reason: Option<Rejection>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    closed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrderFill {
    pub price: f64,
    pub qty: f64,
    pub fee: f64,
    pub fee_asset: Option<String>,
    pub ts: DateTime<Utc>,
}

impl OrderDetail {
    #[allow(dead_code)]
    fn from_query(exchange: Exchange, transaction_id: Option<String>, add_order: AddOrderRequest) -> Self {
        let pair_string = add_order.pair.to_string();
        let (base_asset, quote_asset) = pair_string.split_once('_').expect("pair string should be BASE_QUOTE");
        let base_asset = base_asset.to_string();
        let quote_asset = quote_asset.to_string();
        Self {
            id: add_order.order_id.expect("order id is required"),
            transaction_id,
            remote_id: None,
            status: OrderStatus::Staged,
            exchange: exchange.to_string(),
            pair: pair_string,
            base_asset,
            quote_asset,
            side: add_order.side,
            order_type: add_order.order_type,
            enforcement: add_order.enforcement,
            base_qty: add_order.quantity,
            quote_qty: add_order.quote_order_qty,
            price: add_order.price,
            stop_price: add_order.stop_price,
            iceberg_qty: add_order.iceberg_qty,
            is_test: add_order.dry_run,
            asset_type: add_order.asset_type.unwrap_or_default(),
            executed_qty: None,
            cummulative_quote_qty: None,
            margin_side_effect: None,
            borrowed_amount: None,
            borrowed_asset: None,
            fills: vec![],
            weighted_price: 0.0,
            total_executed_qty: 0.0,
            rejection_reason: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            closed_at: None,
        }
    }

    #[allow(dead_code)]
    fn from_submission(&mut self, submission: OrderSubmission) {
        self.executed_qty = Some(submission.executed_qty);
        self.cummulative_quote_qty = Some(submission.cummulative_quote_qty);
        self.borrowed_asset = submission.borrow_asset;
        self.borrowed_amount = submission.borrowed_amount;
        self.remote_id = Some(submission.id);
        self.status = OrderStatus::Created;
        self.updated_at = Utc::now();
    }

    #[allow(dead_code)]
    fn from_fill_update(&mut self, update: OrderUpdate) {
        let fill = OrderFill {
            price: update.last_executed_price,
            qty: update.last_executed_qty,
            fee: update.commission,
            fee_asset: update.commission_asset,
            ts: Utc.timestamp_millis(update.timestamp as i64),
        };
        self.fills.push(fill);
        self.cummulative_quote_qty = Some(update.cummulative_quote_asset_transacted_qty);
        self.total_executed_qty = update.cummulative_filled_qty;
        self.status = update.new_status.into();
        if self.status == OrderStatus::Filled {
            self.closed_at = Some(Utc::now());
        }
        self.updated_at = Utc::now();
        self.weighted_price = self.fills.iter().map(|fill| fill.price * fill.qty).sum::<f64>()
            / self.fills.iter().map(|fill| fill.qty).sum::<f64>();
    }

    #[allow(dead_code)]
    fn from_rejected(&mut self, rejection: Rejection) {
        self.rejection_reason = Some(rejection);
        self.status = OrderStatus::Rejected;
        self.updated_at = Utc::now();
    }
}

#[cfg(test)]
mod test {
    use fake::Fake;
    use quickcheck::{Arbitrary, Gen};

    use crate::types::BookPosition;

    lazy_static! {
        static ref MAX_ALLOWED_F64: f64 = 1.0_f64.powf(10.0);
    }

    fn zero_if_nan(x: f64) -> f64 {
        if x.is_nan() || x.is_infinite() {
            0.0
        } else {
            x
        }
    }

    impl Arbitrary for BookPosition {
        fn arbitrary(g: &mut Gen) -> BookPosition {
            BookPosition {
                ask: zero_if_nan(f64::arbitrary(g)),
                ask_q: zero_if_nan(f64::arbitrary(g)),
                bid: zero_if_nan(f64::arbitrary(g)),
                bid_q: zero_if_nan(f64::arbitrary(g)),
                mid: zero_if_nan(f64::arbitrary(g)),
                event_time: fake::faker::chrono::en::DateTime().fake(),
            }
        }
    }
}
