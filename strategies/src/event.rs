use crate::types::{OperationEvent, TradeEvent};
use itertools::Itertools;
use portfolio::portfolio::Portfolio;
use trading::position::{OperationKind, Position};

pub fn trades_history(portfolio: &Portfolio) -> Vec<(OperationEvent, TradeEvent)> {
    let mut trade_events: Vec<(OperationEvent, TradeEvent)> = Vec::new();
    for pos in portfolio.positions_history().unwrap() {
        if let Some((op, event)) = open_events(&pos) {
            trade_events.push((op, event));
        }
        if let Some((op, event)) = close_events(&pos) {
            trade_events.push((op, event));
        }
    }
    trade_events.into_iter().sorted_by_key(|o| o.1.at).collect()
}

pub fn open_events(pos: &Position) -> Option<(OperationEvent, TradeEvent)> {
    pos.open_order.as_ref().map(|order| {
        (
            OperationEvent {
                op: OperationKind::Open,
                pos: pos.kind,
                at: order.closed_at.unwrap(),
            },
            TradeEvent {
                side: order.side.into(),
                qty: order.base_qty.unwrap_or(0.0),
                pair: pos.symbol.to_string(),
                price: order.price.unwrap_or(0.0),
                strat_value: pos.meta.exit_equity_point.as_ref().map(|ep| ep.equity).unwrap_or(0.0),
                at: order.closed_at.unwrap(),
                borrowed: order.borrowed_amount,
                interest: None,
            },
        )
    })
}

pub fn close_events(pos: &Position) -> Option<(OperationEvent, TradeEvent)> {
    pos.close_order.as_ref().map(|order| {
        (
            OperationEvent {
                op: OperationKind::Close,
                pos: pos.kind,
                at: order.closed_at.unwrap(),
            },
            TradeEvent {
                side: order.side.into(),
                qty: order.base_qty.unwrap_or(0.0),
                pair: pos.symbol.to_string(),
                price: order.price.unwrap_or(0.0),
                strat_value: pos.meta.exit_equity_point.as_ref().map(|ep| ep.equity).unwrap_or(0.0),
                at: order.closed_at.unwrap(),
                borrowed: order.borrowed_amount,
                interest: Some(pos.interests),
            },
        )
    })
}
