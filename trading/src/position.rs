// --------- Data Types ---------

use chrono::{DateTime, Utc};
use coinnect_rt::prelude::{Exchange, MarketEvent, MarketEventEnvelope, TradeType};
use coinnect_rt::types::Pair;
use std::str::FromStr;
use util::time::now;
use uuid::Uuid;

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

#[derive(Clone, Copy, PartialEq, Eq, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
            enter_trace_id: Default::default(),
            open_at: Utc::now(),
            last_update_trace_id: Default::default(),
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

    /// +ve or -ve quantity of symbol contracts opened.
    pub quantity: f64,

    pub open_order: Option<OrderDetail>,

    pub close_order: Option<OrderDetail>,

    /// Symbol current close price.
    pub current_symbol_price: f64,

    /// Unrealised PnL whilst the [Position] is open.
    pub unreal_profit_loss: f64,

    /// Realised PnL after the [Position] has closed.
    pub result_profit_loss: f64,
}

impl Default for Position {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            meta: Default::default(),
            exchange: Exchange::Binance,
            symbol: "BTC_USDT".into(),
            kind: PositionKind::default(),
            quantity: 0.0,
            open_order: None,
            close_order: None,
            current_symbol_price: 0.0,
            unreal_profit_loss: 0.0,
            result_profit_loss: 0.0,
        }
    }
}

impl Position {
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
            open_order: Some(order.clone()),
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
        self.close_order = Some(order.clone());
        self.current_symbol_price = order.price.unwrap_or(0.0);
        self.result_profit_loss = self.calculate_result_profit_loss();
        self.unreal_profit_loss = self.result_profit_loss;
    }

    pub fn update(&mut self, event: MarketEventEnvelope) {
        let price = match event.e {
            MarketEvent::Trade(ref t) => t.price,
            MarketEvent::Orderbook(ref o) => o.avg_price().unwrap_or(0.0),
            MarketEvent::CandleTick(ref ct) => ct.close,
            MarketEvent::Noop => 0.0,
        };
        self.meta.last_update_trace_id = event.trace_id;
        self.meta.last_update = event.e.time();
        self.current_symbol_price = price;
        self.unreal_profit_loss = self.calculate_unreal_profit_loss();
    }

    pub fn current_value_gross(&self) -> f64 {
        self.open_order
            .as_ref()
            .map(|o| o.total_executed_qty)
            .unwrap_or(0.0)
            .abs()
            * self.current_symbol_price
    }

    /// Calculate the approximate [Position::unreal_profit_loss] of a [Position].
    pub fn calculate_unreal_profit_loss(&self) -> f64 {
        let enter_value = self.open_quote_value();
        // missing interests and exit fees
        match self.kind {
            PositionKind::Long => self.current_value_gross() - enter_value,
            PositionKind::Short => enter_value - self.current_value_gross(),
        }
    }

    fn open_quote_value(&self) -> f64 { self.open_order.as_ref().map(|o| o.realized_quote_value()).unwrap() }

    fn close_quote_value(&self) -> f64 { self.close_order.as_ref().map(|o| o.realized_quote_value()).unwrap() }

    /// Calculate the exact [Position::result_profit_loss] of a [Position].
    pub fn calculate_result_profit_loss(&self) -> f64 {
        let exit_value = self.close_quote_value();
        let enter_value = self.open_quote_value();
        match self.kind {
            PositionKind::Long => exit_value - enter_value,
            PositionKind::Short => enter_value - exit_value,
        }
    }

    /// Calculate the PnL return of a closed [Position] - assumed [Position::result_profit_loss] is
    /// appropriately calculated.
    pub fn calculate_profit_loss_return(&self) -> f64 { self.result_profit_loss / self.open_quote_value() }

    pub fn is_failed_open(&self) -> bool { self.open_order.as_ref().map(|o| o.is_rejected()).unwrap_or(false) }

    pub fn is_opened(&self) -> bool { self.open_order.as_ref().map(|o| o.is_filled()).unwrap_or(false) }

    pub fn is_closed(&self) -> bool { self.close_order.as_ref().map(|o| o.is_filled()).unwrap_or(false) }

    pub fn quantity(&self) -> f64 { self.open_order.as_ref().map(|o| o.total_executed_qty).unwrap_or(0.0) }
}

/// Equity value at a point in time.
#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
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
    /// Updates using the input [Position]'s PnL & associated timestamp.
    pub fn update(&mut self, position: &Position) {
        match position.meta.close_at {
            None => {
                // Position is not exited
                self.equity += position.unreal_profit_loss;
                self.timestamp = position.meta.last_update;
            }
            Some(exit_timestamp) => {
                self.equity += position.result_profit_loss;
                self.timestamp = exit_timestamp;
            }
        }
    }
}
