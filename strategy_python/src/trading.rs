#![allow(clippy::needless_option_as_deref)]

use std::str::FromStr;

use chrono::{TimeZone, Utc};
use pyo3::exceptions::PyBaseException;
use pyo3::prelude::*;
use uuid::Uuid;

use strategy::coinnect::prelude::*;
use strategy::coinnect::types::{AssetType, MarginSideEffect, OrderType};
use strategy::trading::position::{OperationKind, PositionKind};
use strategy::trading::signal::{ExecutionInstruction, TradeSignal};
use strategy::trading::types::TradeKind;
use util::time::now;

#[pyclass(name = "TradeSignal", module = "trading", subclass)]
#[derive(Debug, Clone)]
#[pyo3(
    text_signature = "TradeSignal(position, operation, side, price, qty, pair, exchange, dry_mode, asset_type, order_type, instructions, enforcement, side_effect, event_time, trace_id, /)"
)]
pub(crate) struct PyTradeSignal {
    inner: TradeSignal,
}

impl From<TradeSignal> for PyTradeSignal {
    fn from(t: TradeSignal) -> Self { Self { inner: t } }
}

#[pymethods]
impl PyTradeSignal {
    #[allow(clippy::too_many_arguments)]
    #[new]
    fn new(
        position: &str,
        operation: &str,
        side: &str,
        price: f64,
        qty: f64,
        pair: &str,
        exchange: &str,
        dry_mode: bool,
        asset_type: &str,
        order_type: &str,
        instructions: Option<&str>,
        enforcement: Option<&str>,
        side_effect: Option<&str>,
        event_time: i64,
        trace_id: &str,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: TradeSignal {
                trace_id: Uuid::from_str(trace_id)
                    .map_err(|_| PyBaseException::new_err(format!("bad uuid string '{}'", trace_id)))?,
                event_time: Utc.timestamp_millis(event_time),
                signal_time: now(),
                pos_kind: PositionKind::from_str(position)
                    .map_err(|_| PyBaseException::new_err(format!("unknown position '{}'", position)))?,
                op_kind: OperationKind::from_str(operation)
                    .map_err(|_| PyBaseException::new_err(format!("unknown operation '{}'", operation)))?,
                trade_kind: TradeKind::from_str(side)
                    .map_err(|_| PyBaseException::new_err(format!("unknown side '{}'", side)))?,
                price,
                qty: Some(qty),
                pair: pair.into(),
                exchange: Exchange::from_str(exchange)
                    .map_err(|_| PyBaseException::new_err(format!("unknown exchange '{}'", exchange)))?,
                instructions: instructions.and_then(|i| {
                    ExecutionInstruction::from_str(i)
                        .map_err(|_| PyBaseException::new_err(format!("unknown execution instruction '{}'", i)))
                        .ok()
                }),
                dry_mode,
                order_type: OrderType::from_str(order_type)
                    .map_err(|_| PyBaseException::new_err(format!("unknown order_type '{}'", exchange)))?,
                enforcement: enforcement.and_then(|e| {
                    OrderEnforcement::from_str(e)
                        .map_err(|_| PyBaseException::new_err(format!("unknown order_type '{}'", exchange)))
                        .ok()
                }),
                asset_type: Some(
                    AssetType::from_str(asset_type)
                        .map_err(|_| PyBaseException::new_err(format!("unknown asset type '{}'", asset_type)))?,
                ),
                side_effect: side_effect.and_then(|e| {
                    MarginSideEffect::from_str(e)
                        .map_err(|_| PyBaseException::new_err(format!("unknown order_type '{}'", exchange)))
                        .ok()
                }),
            },
        })
    }
}
