use chrono::{DateTime, Utc};
use uuid::Uuid;

use coinnect_rt::prelude::*;
use coinnect_rt::types::MarginSideEffect;
use util::time::now;

use crate::position::{OperationKind, PositionKind};
use crate::types::{OrderConf, OrderMode, TradeKind};

#[derive(Debug, Clone)]
pub struct TradeSignal {
    /// Trace of the event that triggered the signal
    pub trace_id: Uuid,
    /// Event time that triggered the signal
    pub event_time: DateTime<Utc>,
    /// Time of the signal itself
    pub signal_time: DateTime<Utc>,
    /// Type of position
    pub pos_kind: PositionKind,
    /// Type of operation
    pub op_kind: OperationKind,
    /// Trade side
    pub trade_kind: TradeKind,
    /// Price
    pub price: f64,
    /// Base quantity
    pub qty: Option<f64>,
    /// Target market pair
    pub pair: Pair,
    /// Target exchange
    pub exchange: Exchange,
    /// Optional additional instructions
    pub instructions: Option<ExecutionInstruction>,
    /// Dry mode (simulate the order, do not execute)
    pub dry_mode: bool,
    /// Order type, will result in error if unsupported by exchange
    pub order_type: OrderType,
    /// Enforcement, default is per exchange
    pub enforcement: Option<OrderEnforcement>,
    /// Asset type, defaults to Spot
    pub asset_type: Option<AssetType>,
    /// Margin side effect type, only set if using [`AssetType::Margin`] or  [`AssetType::IsolatedMargin`]
    pub side_effect: Option<MarginSideEffect>,
}

impl Default for TradeSignal {
    fn default() -> Self {
        Self {
            trace_id: Uuid::default(),
            pos_kind: PositionKind::default(),
            op_kind: OperationKind::Open,
            trade_kind: TradeKind::Buy,
            event_time: now(),
            signal_time: now(),
            price: 0.0,
            qty: Some(0.0),
            pair: "BTC_USDT".into(),
            exchange: Exchange::Binance,
            instructions: None,
            dry_mode: false,
            order_type: OrderType::default(),
            enforcement: None,
            asset_type: None,
            side_effect: None,
        }
    }
}

impl TradeSignal {
    pub fn xch_and_pair(&self) -> (Exchange, Pair) { (self.exchange, self.pair.clone()) }
}

impl<'a> From<&'a TradeSignal> for AddOrderRequest {
    fn from(t: &'a TradeSignal) -> Self {
        let side = match (t.pos_kind, t.op_kind) {
            (PositionKind::Short, OperationKind::Open) | (PositionKind::Long, OperationKind::Close) => TradeType::Sell,
            (PositionKind::Short, OperationKind::Close) | (PositionKind::Long, OperationKind::Open) => TradeType::Buy,
        };
        Self {
            pair: t.pair.clone(),
            side,
            order_type: t.order_type,
            enforcement: t.enforcement,
            quantity: t.qty,
            price: Some(t.price),
            order_id: Uuid::new_v4().to_string(),
            dry_run: t.dry_mode,
            asset_type: t.asset_type,
            side_effect_type: t.side_effect,
            ..AddOrderRequest::default()
        }
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, EnumString)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionInstruction {
    CancelIfNotBest,
    DoNotIncrease,
    DoNotReduce,
    LastPrice,
}

#[allow(clippy::too_many_arguments)]
pub fn new_trade_signal(
    pair: Pair,
    exchange: Exchange,
    order_conf: &OrderConf,
    event_time: DateTime<Utc>,
    trace_id: Uuid,
    op_kind: OperationKind,
    pos_kind: PositionKind,
    price: f64,
    qty: Option<f64>,
) -> TradeSignal {
    let trade_kind = match (pos_kind, op_kind) {
        (PositionKind::Short, OperationKind::Open) | (PositionKind::Long, OperationKind::Close) => TradeKind::Sell,
        (PositionKind::Long, OperationKind::Open) | (PositionKind::Short, OperationKind::Close) => TradeKind::Buy,
    };
    let margin_side_effect = if order_conf.asset_type.is_margin() && pos_kind == PositionKind::Short {
        if op_kind == OperationKind::Open {
            Some(MarginSideEffect::MarginBuy)
        } else {
            Some(MarginSideEffect::AutoRepay)
        }
    } else {
        None
    };
    let (order_type, enforcement) = match order_conf.order_mode {
        OrderMode::Limit => (OrderType::Limit, Some(OrderEnforcement::FOK)),
        OrderMode::Market => (OrderType::Market, None),
    };
    TradeSignal {
        trace_id,
        pos_kind,
        op_kind,
        trade_kind,
        event_time,
        signal_time: now(),
        price,
        qty,
        pair,
        exchange,
        instructions: None,
        dry_mode: order_conf.dry_mode,
        order_type,
        enforcement,
        asset_type: Some(order_conf.asset_type),
        side_effect: margin_side_effect,
    }
}
