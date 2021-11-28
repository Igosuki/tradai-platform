#[cfg(test)]
use crate::types::{OperationEvent, TradeEvent};
#[cfg(test)]
use trading::position::{OperationKind, Position};

#[cfg(test)]
pub fn open_events(pos: &Position) -> Option<(OperationEvent, TradeEvent)> {
    pos.open_order.as_ref().map(|order| {
        (
            OperationEvent {
                op: OperationKind::Open,
                pos: pos.kind,
                at: order.created_at,
            },
            TradeEvent {
                op: order.side.into(),
                qty: order.base_qty.unwrap_or(0.0),
                pair: pos.symbol.to_string(),
                price: order.price.unwrap_or(0.0),
                strat_value: 0.0,
                at: order.created_at,
                borrowed: order.borrowed_amount,
                interest: None,
            },
        )
    })
}

#[cfg(test)]
pub fn close_events(pos: &Position) -> Option<(OperationEvent, TradeEvent)> {
    pos.close_order.as_ref().map(|order| {
        (
            OperationEvent {
                op: OperationKind::Close,
                pos: pos.kind,
                at: order.created_at,
            },
            TradeEvent {
                op: order.side.into(),
                qty: order.base_qty.unwrap_or(0.0),
                pair: pos.symbol.to_string(),
                price: order.price.unwrap_or(0.0),
                strat_value: 0.0,
                at: order.created_at,
                borrowed: order.borrowed_amount,
                interest: Some(pos.interests),
            },
        )
    })
}
