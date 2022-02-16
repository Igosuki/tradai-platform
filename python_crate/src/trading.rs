#![allow(clippy::needless_option_as_deref)]

use std::str::FromStr;

use chrono::{TimeZone, Utc};
use pyo3::exceptions::PyBaseException;
use pyo3::prelude::*;
use pyo3_chrono::NaiveDateTime;

use brokers::prelude::*;
use brokers::types::{AssetType, MarginSideEffect, OrderType};
use ext::MapInto;
use trading::position::Position;
use trading::position::{OperationKind, PositionKind};
use trading::signal::{ExecutionInstruction, TradeSignal};
use trading::types::TradeKind;
use util::time::now;

use crate::uuid::Uuid;

#[pyclass(name = "TradeSignal", module = "trading", subclass)]
#[derive(Debug, Clone)]
#[pyo3(
    text_signature = "TradeSignal(position, operation, side, price, pair, exchange, dry_mode, asset_type, order_type, event_time, trace_id, qty, instructions, enforcement, side_effect, /)"
)]
pub(crate) struct PyTradeSignal {
    inner: TradeSignal,
}

impl From<TradeSignal> for PyTradeSignal {
    fn from(t: TradeSignal) -> Self { Self { inner: t } }
}

impl From<PyTradeSignal> for TradeSignal {
    fn from(t: PyTradeSignal) -> Self { t.inner }
}

#[pymethods]
impl PyTradeSignal {
    #[allow(clippy::too_many_arguments)]
    #[new]
    fn new(
        position: PyPositionKind,
        operation: PyOperationKind,
        side: PyTradeKind,
        price: f64,
        pair: &str,
        exchange: &str,
        dry_mode: bool,
        asset_type: PyAssetType,
        order_type: PyOrderType,
        event_time: NaiveDateTime,
        trace_id: Uuid,
        qty: Option<f64>,
        instructions: Option<PyExecutionInstruction>,
        enforcement: Option<PyOrderEnforcement>,
        side_effect: Option<PyMarginSideEffect>,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: TradeSignal {
                trace_id: trace_id.handle,
                event_time: Utc.from_utc_datetime(&event_time.0),
                signal_time: now(),
                pos_kind: position.into(),
                op_kind: operation.into(),
                trade_kind: side.into(),
                price,
                qty,
                pair: pair.to_uppercase().into(),
                exchange: Exchange::from_str(exchange)
                    .map_err(|_| PyBaseException::new_err(format!("unknown exchange '{}'", exchange)))?,
                instructions: instructions.map_into(),
                dry_mode,
                order_type: order_type.into(),
                enforcement: enforcement.map_into(),
                asset_type: Some(asset_type.into()),
                side_effect: side_effect.map_into(),
            },
        })
    }
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
pub(crate) fn signal(
    position: PyPositionKind,
    operation: PyOperationKind,
    side: PyTradeKind,
    price: f64,
    pair: &str,
    exchange: &str,
    dry_mode: bool,
    asset_type: PyAssetType,
    order_type: PyOrderType,
    event_time: NaiveDateTime,
    trace_id: Uuid,
    qty: Option<f64>,
    instructions: Option<PyExecutionInstruction>,
    enforcement: Option<PyOrderEnforcement>,
    side_effect: Option<PyMarginSideEffect>,
) -> PyResult<PyTradeSignal> {
    PyTradeSignal::new(
        position,
        operation,
        side,
        price,
        pair,
        exchange,
        dry_mode,
        asset_type,
        order_type,
        event_time,
        trace_id,
        qty,
        instructions,
        enforcement,
        side_effect,
    )
}

#[pyclass(name = "PositionKind")]
#[derive(Copy, Clone)]
pub(crate) enum PyPositionKind {
    Short,
    Long,
}

impl From<PyPositionKind> for PositionKind {
    fn from(p: PyPositionKind) -> Self {
        match p {
            PyPositionKind::Short => PositionKind::Short,
            PyPositionKind::Long => PositionKind::Long,
        }
    }
}

#[pyclass(name = "TradeKind")]
#[derive(Copy, Clone)]
pub(crate) enum PyTradeKind {
    Buy,
    Sell,
}

impl From<PyTradeKind> for TradeKind {
    fn from(t: PyTradeKind) -> Self {
        match t {
            PyTradeKind::Buy => TradeKind::Buy,
            PyTradeKind::Sell => TradeKind::Sell,
        }
    }
}

#[pyclass(name = "OperationKind")]
#[derive(Copy, Clone)]
pub(crate) enum PyOperationKind {
    Open,
    Close,
}

impl From<PyOperationKind> for OperationKind {
    fn from(o: PyOperationKind) -> Self {
        match o {
            PyOperationKind::Open => OperationKind::Open,
            PyOperationKind::Close => OperationKind::Close,
        }
    }
}

#[pyclass(name = "OrderType")]
#[derive(Copy, Clone)]
pub(crate) enum PyOrderType {
    Limit,
    Market,
    StopLoss,
    StopLossLimit,
    TakeProfit,
    TakeProfitLimit,
    LimitMaker,
}

impl From<PyOrderType> for OrderType {
    fn from(o: PyOrderType) -> Self {
        match o {
            PyOrderType::Limit => OrderType::Limit,
            PyOrderType::Market => OrderType::Market,
            PyOrderType::StopLoss => OrderType::StopLoss,
            PyOrderType::StopLossLimit => OrderType::StopLossLimit,
            PyOrderType::TakeProfit => OrderType::TakeProfit,
            PyOrderType::TakeProfitLimit => OrderType::TakeProfitLimit,
            PyOrderType::LimitMaker => OrderType::LimitMaker,
        }
    }
}

#[pyclass(name = "ExecutionInstruction")]
#[derive(Copy, Clone)]
pub(crate) enum PyExecutionInstruction {
    CancelIfNotBest,
    DoNotIncrease,
    DoNotReduce,
    LastPrice,
}

impl From<PyExecutionInstruction> for ExecutionInstruction {
    fn from(e: PyExecutionInstruction) -> Self {
        match e {
            PyExecutionInstruction::CancelIfNotBest => ExecutionInstruction::CancelIfNotBest,
            PyExecutionInstruction::DoNotIncrease => ExecutionInstruction::DoNotIncrease,
            PyExecutionInstruction::DoNotReduce => ExecutionInstruction::DoNotReduce,
            PyExecutionInstruction::LastPrice => ExecutionInstruction::LastPrice,
        }
    }
}

#[pyclass(name = "MarginSideEffect")]
#[derive(Copy, Clone)]
pub(crate) enum PyMarginSideEffect {
    NoSideEffect,
    MarginBuy,
    AutoRepay,
}

impl From<PyMarginSideEffect> for MarginSideEffect {
    fn from(m: PyMarginSideEffect) -> Self {
        match m {
            PyMarginSideEffect::NoSideEffect => MarginSideEffect::NoSideEffect,
            PyMarginSideEffect::MarginBuy => MarginSideEffect::MarginBuy,
            PyMarginSideEffect::AutoRepay => MarginSideEffect::AutoRepay,
        }
    }
}

#[pyclass(name = "AssetType")]
#[derive(Copy, Clone)]
pub enum PyAssetType {
    Spot,
    Margin,
    MarginFunding,
    IsolatedMargin,
    Index,
    Binary,
    PerpetualContract,
    PerpetualSwap,
    Futures,
    UpsideProfitContract,
    DownsideProfitContract,
    CoinMarginedFutures,
    UsdtMarginedFutures,
}

impl From<PyAssetType> for AssetType {
    fn from(a: PyAssetType) -> Self {
        match a {
            PyAssetType::Spot => AssetType::Spot,
            PyAssetType::Margin => AssetType::Margin,
            PyAssetType::MarginFunding => AssetType::MarginFunding,
            PyAssetType::IsolatedMargin => AssetType::IsolatedMargin,
            PyAssetType::Index => AssetType::Index,
            PyAssetType::Binary => AssetType::Binary,
            PyAssetType::PerpetualContract => AssetType::PerpetualContract,
            PyAssetType::PerpetualSwap => AssetType::PerpetualSwap,
            PyAssetType::Futures => AssetType::Futures,
            PyAssetType::UpsideProfitContract => AssetType::UpsideProfitContract,
            PyAssetType::DownsideProfitContract => AssetType::DownsideProfitContract,
            PyAssetType::CoinMarginedFutures => AssetType::CoinMarginedFutures,
            PyAssetType::UsdtMarginedFutures => AssetType::UsdtMarginedFutures,
        }
    }
}

/// How an order will be resolved
#[pyclass(name = "OrderEnforcement")]
#[derive(Copy, Clone)]
#[allow(clippy::upper_case_acronyms)]
pub enum PyOrderEnforcement {
    /// Good Till Canceled
    GTC,
    /// Immediate Or Cancel
    IOC,
    /// Fill or Kill
    FOK,
    /// Good till executed
    GTX,
}

impl From<PyOrderEnforcement> for OrderEnforcement {
    fn from(o: PyOrderEnforcement) -> Self {
        match o {
            PyOrderEnforcement::GTC => OrderEnforcement::GTC,
            PyOrderEnforcement::IOC => OrderEnforcement::IOC,
            PyOrderEnforcement::FOK => OrderEnforcement::FOK,
            PyOrderEnforcement::GTX => OrderEnforcement::GTX,
        }
    }
}

#[pyclass(name = "Position", module = "strategy", subclass)]
#[derive(Debug, Clone)]
pub(crate) struct PyPosition {
    pub(crate) inner: Position,
}

#[pymethods]
impl PyPosition {
    fn debug(&self) {
        info!("{:?}", self);
    }
}

impl From<PyPosition> for Position {
    fn from(event: PyPosition) -> Position { event.inner }
}

impl From<Position> for PyPosition {
    fn from(e: Position) -> Self { PyPosition { inner: e } }
}
