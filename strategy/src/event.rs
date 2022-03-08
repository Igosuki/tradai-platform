use chrono::{TimeZone, Utc};
use itertools::Itertools;

use crate::types::{OperationEvent, TradeEvent};
use trading::position::{OperationKind, Position};

pub fn trades_history(history: &[Position]) -> Vec<(OperationEvent, TradeEvent)> {
    let mut trade_events: Vec<(OperationEvent, TradeEvent)> = Vec::new();
    for pos in history {
        if let Some((op, event)) = open_events(pos) {
            trade_events.push((op, event));
        }
        if let Some((op, event)) = close_events(pos) {
            trade_events.push((op, event));
        }
    }
    trade_events.into_iter().sorted_by_key(|o| o.1.at).collect()
}

/// # Panics
///
/// If the open order has no closed timestamp
pub fn open_events(pos: &Position) -> Option<(OperationEvent, TradeEvent)> {
    pos.open_order.as_ref().map(|order| {
        (
            OperationEvent {
                op: OperationKind::Open,
                pos: pos.kind,
                at: pos.meta.open_at,
            },
            TradeEvent {
                side: order.side.into(),
                qty: order.base_qty.unwrap_or(0.0),
                pair: pos.symbol.to_string(),
                price: order.price.unwrap_or(0.0),
                strat_value: pos.meta.exit_equity_point.as_ref().map_or(0.0, |ep| ep.equity),
                at: pos.meta.open_at,
                borrowed: order.borrowed_amount,
                interest: None,
            },
        )
    })
}

/// # Panics
///
/// if the close order has no close timestamp
pub fn close_events(pos: &Position) -> Option<(OperationEvent, TradeEvent)> {
    pos.close_order.as_ref().map(|order| {
        (
            OperationEvent {
                op: OperationKind::Close,
                pos: pos.kind,
                at: pos.meta.close_at.unwrap_or_else(|| Utc.timestamp_millis(0)),
            },
            TradeEvent {
                side: order.side.into(),
                qty: order.base_qty.unwrap_or(0.0),
                pair: pos.symbol.to_string(),
                price: order.price.unwrap_or(0.0),
                strat_value: pos.meta.exit_equity_point.as_ref().map_or(0.0, |ep| ep.equity),
                at: pos.meta.close_at.unwrap_or_else(|| Utc.timestamp_millis(0)),
                borrowed: order.borrowed_amount,
                interest: Some(pos.interests),
            },
        )
    })
}
