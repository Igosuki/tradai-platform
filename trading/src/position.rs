// --------- Data Types ---------

use std::str::FromStr;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use coinnect_rt::prelude::{Exchange, MarketEvent, MarketEventEnvelope, TradeType};
use coinnect_rt::types::{AssetType, Pair};
use util::time::now;

use crate::order_manager::types::OrderDetail;

#[derive(
    Display, Copy, Eq, PartialEq, Clone, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum,
)]
#[serde(rename_all = "lowercase")]
pub enum PositionKind {
    #[strum(serialize = "short")]
    Short,
    #[strum(serialize = "long")]
    Long,
}

impl Default for PositionKind {
    fn default() -> Self { Self::Long }
}

impl PositionKind {
    pub fn is_short(&self) -> bool { matches!(self, PositionKind::Short) }

    pub fn is_long(&self) -> bool { matches!(self, PositionKind::Long) }
}

#[derive(
    Display, Clone, Copy, PartialEq, Eq, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationKind {
    #[strum(serialize = "open")]
    Open,
    #[strum(serialize = "close")]
    Close,
}

impl OperationKind {
    pub fn is_open(&self) -> bool { matches!(self, OperationKind::Open) }

    pub fn is_close(&self) -> bool { matches!(self, OperationKind::Close) }
}

/// Metadata detailing the trace UUIDs & timestamps associated with entering, updating & exiting
/// a [Position].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, juniper::GraphQLObject)]
pub struct PositionMeta {
    /// Trace UUID of the MarketEvent that triggered the entering of this [Position].
    pub enter_trace_id: Uuid,

    /// MarketEvent Bar timestamp that triggered the entering of this [Position].
    pub open_at: DateTime<Utc>,

    /// Trace UUID of the last event to trigger a [Position] update.
    pub last_update_trace_id: Uuid,

    /// Event timestamp of the last event to trigger a [Position] update.9
    pub last_update: DateTime<Utc>,

    /// Trace UUID of the MarketEvent that triggered the exiting of this [Position].
    pub close_trace_id: Option<Uuid>,

    /// MarketEvent Bar timestamp that triggered the exiting of this [Position].
    pub close_at: Option<DateTime<Utc>>,

    /// Portfolio [EquityPoint] calculated after the [Position] exit.
    pub exit_equity_point: Option<EquityPoint>,
}

impl Default for PositionMeta {
    fn default() -> Self {
        Self {
            enter_trace_id: Uuid::default(),
            open_at: Utc::now(),
            last_update_trace_id: Uuid::default(),
            last_update: Utc::now(),
            close_trace_id: None,
            close_at: None,
            exit_equity_point: None,
        }
    }
}

/// Data encapsulating the state of an ongoing or closed [Position].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Position {
    /// A unique ID for this position
    pub id: Uuid,

    /// Metadata detailing trace UUIDs, timestamps & equity associated with entering, updating & exiting.
    pub meta: PositionMeta,

    /// Target exchange.
    pub exchange: Exchange,

    /// Market symbol.
    pub symbol: Pair,

    /// Long or Short.
    pub kind: PositionKind,

    /// Asset Type.
    pub asset_type: AssetType,

    /// +ve or -ve quantity of symbol contracts opened.
    pub quantity: f64,

    /// executed quantity @ open
    pub open_executed_qty: f64,

    /// weighted price @ open
    pub open_weighted_price: f64,

    /// quote value @ open
    pub open_quote_value: f64,

    /// base fees @ open
    pub open_base_fees: f64,

    /// quote value @ close
    pub close_quote_value: f64,

    /// Symbol current close price.
    pub current_price: f64,

    /// Unrealised PnL whilst the [Position] is open.
    pub unrealized_pl: f64,

    /// Realised PnL after the [Position] has closed.
    pub result_pl: f64,

    /// Accrued Interest
    pub interests: f64,

    /// Open order id
    pub open_order_id: String,

    /// Close order id
    pub close_order_id: Option<String>,
}

#[juniper::graphql_object]
impl Position {
    fn id(&self) -> String { self.id.to_string() }

    fn exchange(&self) -> String { self.exchange.to_string() }

    fn symbol(&self) -> String { self.symbol.to_string() }

    fn kind(&self) -> PositionKind { self.kind }

    fn qty(&self) -> f64 { self.quantity }

    fn open_order_id(&self) -> String { self.open_order_id.clone() }

    fn close_order_id(&self) -> Option<String> { self.close_order_id.clone() }

    fn current_price(&self) -> f64 { self.current_price }

    fn pnl(&self) -> f64 { self.result_pl }

    fn unreal_pnl(&self) -> f64 { self.unrealized_pl }
}

impl Default for Position {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            meta: PositionMeta::default(),
            exchange: Exchange::Binance,
            symbol: "BTC_USDT".into(),
            kind: PositionKind::default(),
            asset_type: Default::default(),
            quantity: 0.0,
            open_executed_qty: 0.0,
            open_weighted_price: 0.0,
            open_quote_value: 0.0,
            open_base_fees: 0.0,
            close_quote_value: 0.0,
            current_price: 0.0,
            unrealized_pl: 0.0,
            result_pl: 0.0,
            interests: 0.0,
            open_order_id: Default::default(),
            close_order_id: Default::default(),
        }
    }
}

impl Position {
    /// Builds a position from an opening order
    ///
    /// # Panics
    ///
    /// if the exchange string cannot be parsed
    pub fn open(order: &OrderDetail) -> Self {
        let kind = match order.side {
            TradeType::Sell => PositionKind::Short,
            TradeType::Buy => PositionKind::Long,
        };
        let trace_id = Uuid::new_v4();
        let now = now();
        Self {
            meta: PositionMeta {
                enter_trace_id: trace_id,
                open_at: now,
                last_update_trace_id: trace_id,
                last_update: now,
                close_trace_id: None,
                close_at: None,
                exit_equity_point: None,
            },
            quantity: order.total_executed_qty,
            exchange: Exchange::from_str(order.exchange.as_str()).unwrap(),
            symbol: order.pair.clone().into(),
            kind,
            open_executed_qty: order.total_executed_qty,
            open_weighted_price: order.weighted_price,
            open_quote_value: order.realized_quote_value(),
            open_order_id: order.id.clone(),
            open_base_fees: order.base_fees(),
            asset_type: order.asset_type,
            ..Position::default()
        }
    }

    pub fn close(&mut self, value: f64, order: &OrderDetail) {
        let trace_id = Uuid::new_v4();
        let now = now();
        self.meta.close_trace_id = Some(trace_id);
        self.meta.close_at = Some(now);
        self.meta.last_update = now;
        self.meta.exit_equity_point = Some(EquityPoint {
            equity: value,
            timestamp: now,
        });
        self.close_order_id = Some(order.id.clone());
        self.close_quote_value = order.realized_quote_value();
        self.current_price = order.price.unwrap_or(0.0);
        self.result_pl = self.calculate_result_profit_loss();
        self.unrealized_pl = self.result_pl;
    }

    pub fn update(&mut self, event: &MarketEventEnvelope, fees_rate: f64, interests: f64) {
        let price = match event.e {
            MarketEvent::Trade(ref t) => t.price,
            MarketEvent::Orderbook(ref o) => o.vwap().unwrap_or(0.0),
            MarketEvent::CandleTick(ref ct) => ct.close,
        };
        self.meta.last_update_trace_id = event.trace_id;
        self.meta.last_update = event.e.time();
        self.current_price = price;
        self.unrealized_pl = self.calculate_unreal_profit_loss(fees_rate, interests);
        self.interests = interests;
    }

    /// Total quote ($) amount of the position
    pub fn market_value(&self) -> f64 { self.open_executed_qty.abs() * self.current_price }

    /// Calculate the approximate [`Position::unreal_profit_loss`] of a [`Position`].
    ///
    /// # Panics
    ///
    /// if there is no open order (this should not happen as an open order is required to create a position)
    pub fn calculate_unreal_profit_loss(&self, fees_rate: f64, interests: f64) -> f64 {
        // (open_qty * price) - fees
        let enter_value = self.open_quote_value;
        // (open_qty * current_price)
        let current_value = self.market_value();
        match self.kind {
            PositionKind::Long => ((current_value * (1.0 - fees_rate)) - enter_value - interests) / enter_value,
            PositionKind::Short => {
                (enter_value - (current_value * (1.0 + fees_rate)) - (interests * self.open_weighted_price))
                    / enter_value
            }
        }
    }

    /// Calculate the exact [`Position::result_profit_loss`] of a [`Position`].
    pub fn calculate_result_profit_loss(&self) -> f64 {
        let exit_value = self.close_quote_value;
        let enter_value = self.open_quote_value;
        match self.kind {
            PositionKind::Long => exit_value - enter_value,
            PositionKind::Short => enter_value - exit_value,
        }
    }

    /// Calculate the `PnL` return of a closed [`Position`] - assumed [`Position::result_profit_loss`] is
    /// appropriately calculated.
    pub fn calculate_profit_loss_return(&self) -> f64 { self.result_pl / self.open_quote_value }

    pub fn is_closed(&self) -> bool { self.close_order_id.is_some() }

    pub fn quantity(&self) -> f64 { self.open_executed_qty }

    pub fn is_short(&self) -> bool { self.kind == PositionKind::Short }

    pub fn is_long(&self) -> bool { self.kind == PositionKind::Long }

    pub fn close_qty(&self, fees_rate: f64, interests: f64) -> f64 {
        match self.kind {
            PositionKind::Short => match self.asset_type {
                AssetType::IsolatedMargin | AssetType::Margin => {
                    (self.open_executed_qty / (1.0 - fees_rate)) + interests
                }
                _ => self.open_executed_qty + self.open_base_fees,
            },
            PositionKind::Long => self.open_executed_qty - self.open_base_fees,
        }
    }
}

/// Equity value at a point in time.
#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize, juniper::GraphQLObject)]
pub struct EquityPoint {
    pub equity: f64,
    pub timestamp: DateTime<Utc>,
}

impl Default for EquityPoint {
    fn default() -> Self {
        Self {
            equity: 0.0,
            timestamp: Utc::now(),
        }
    }
}

impl EquityPoint {
    /// Updates using the input [`Position`]'s `PnL` & associated timestamp.
    pub fn update(&mut self, position: &Position) {
        match position.meta.close_at {
            None => {
                // Position is not exited
                self.equity += position.unrealized_pl;
                self.timestamp = position.meta.last_update;
            }
            Some(exit_timestamp) => {
                self.equity += position.result_pl;
                self.timestamp = exit_timestamp;
            }
        }
    }
}
