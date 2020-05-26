use crate::error::Error;
use crate::naive_pair_trading::state::OperationKind;
use anyhow::Result;
use coinnect_rt::exchange::ExchangeApi;
use coinnect_rt::types::{AddOrderRequest, OrderInfo, OrderQuery, Pair, TradeType};
use futures::lock::{Mutex, MutexGuard};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct OrderManager {
    api: Arc<Mutex<Box<dyn ExchangeApi>>>,
}

impl OrderManager {
    pub(crate) fn new(api: Arc<Mutex<Box<dyn ExchangeApi>>>) -> Self {
        OrderManager { api }
    }

    pub(crate) async fn stage_order(
        &self,
        op_kind: OperationKind,
        pair: Pair,
        qty: f64,
        price: f64,
    ) -> Result<OrderInfo> {
        let mut remote_api: MutexGuard<Box<dyn ExchangeApi>> = self.api.lock().await;
        let side = match op_kind {
            OperationKind::BUY => TradeType::Buy,
            OperationKind::SELL => TradeType::Sell,
            _ => unimplemented!(),
        };
        let add_order = OrderQuery::AddOrder(AddOrderRequest {
            pair,
            side,
            quantity: Some(qty),
            price: Some(price),
            ..AddOrderRequest::default()
        });
        remote_api
            .order(add_order)
            .await
            .map_err(|e| anyhow!("Coinnect error {0}", e))
    }
}
