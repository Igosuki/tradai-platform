#![allow(clippy::needless_option_as_deref)]

use chrono::{DateTime, TimeZone, Utc};
use std::str::FromStr;

use pyo3::exceptions::PyBaseException;
use pyo3::prelude::*;
use uuid::Uuid;

use crate::position::{OperationKind, PositionKind};
use crate::signal::{ExecutionInstruction, TradeSignal};
use crate::types::TradeKind;
use coinnect_rt::exchange::Exchange;
use coinnect_rt::prelude::{MarketEventEnvelope, OrderEnforcement};
use coinnect_rt::types::{AssetType, MarginSideEffect, MarketEvent, OrderType};
use util::time::now;

#[pymethods]
impl TradeSignal {
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
            qty,
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
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type")]
#[pyclass]
pub struct PyMarketEventEnvelope {
    pub xch: String,
    pub pair: String,
    pub trace_id: String,
    pub ts: DateTime<Utc>,
    pub e: MarketEvent,
}

impl<'a> From<&'a MarketEventEnvelope> for PyMarketEventEnvelope {
    fn from(e: &'a MarketEventEnvelope) -> Self {
        Self {
            xch: e.xch.to_string(),
            pair: e.pair.to_string(),
            trace_id: e.trace_id.to_string(),
            ts: e.ts,
            e: e.e.clone(),
        }
    }
}
