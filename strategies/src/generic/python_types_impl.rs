#![allow(clippy::needless_option_as_deref)]

use std::str::FromStr;

use pyo3::exceptions::PyBaseException;
use pyo3::prelude::*;

use coinnect_rt::exchange::Exchange;
use coinnect_rt::types::AssetType;

use crate::generic::{InputEvent, TradeSignal};
use crate::types::*;

#[pymethods]
impl TradeSignal {
    #[allow(clippy::too_many_arguments)]
    #[new]
    fn new(
        position: &str,
        operation: &str,
        side: &str,
        price: f64,
        pair: &str,
        exchange: &str,
        dry_mode: bool,
        asset_type: &str,
        instructions: Option<&str>,
    ) -> PyResult<Self> {
        Ok(Self {
            position_kind: PositionKind::from_str(position)
                .map_err(|_| PyBaseException::new_err(format!("unknown position '{}'", position)))?,
            operation_kind: OperationKind::from_str(operation)
                .map_err(|_| PyBaseException::new_err(format!("unknown operation '{}'", operation)))?,
            trade_kind: TradeKind::from_str(side)
                .map_err(|_| PyBaseException::new_err(format!("unknown side '{}'", side)))?,
            price,
            pair: pair.into(),
            exchange: Exchange::from_str(exchange)
                .map_err(|_| PyBaseException::new_err(format!("unknown exchange '{}'", exchange)))?,
            instructions: instructions.and_then(|i| {
                ExecutionInstruction::from_str(i)
                    .map_err(|_| PyBaseException::new_err(format!("unknown execution instruction '{}'", i)))
                    .ok()
            }),
            dry_mode,
            asset_type: AssetType::from_str(asset_type)
                .map_err(|_| PyBaseException::new_err(format!("unknown asset type '{}'", asset_type)))?,
        })
    }
}

impl ToPyObject for InputEvent {
    fn to_object(&self, py: Python) -> PyObject {
        match self {
            InputEvent::BookPosition(bp) => IntoPy::into_py(bp.clone(), py),
            //putEvent::BookPositions(bp) => python! {'bp},
            _ => unimplemented!(),
        }
    }
}
