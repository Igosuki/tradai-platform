use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use actix::{Actor, Addr};
use chrono::Utc;
use httpmock::Method::POST;
use httpmock::{Mock, MockServer};
use rand::random;

use binance::rest_model::TimeInForce;
use coinnect_rt::exchange::MockExchangeApi;
use coinnect_rt::pair::register_pair_default;
use coinnect_rt::prelude::*;
use db::DbOptions;
use ext::prelude::*;

use crate::order_manager::types::OrderDetail;
use crate::order_manager::{OrderManager, OrderManagerClient};

pub fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

pub async fn it_order_manager<S: AsRef<Path>, S2: AsRef<Path>>(
    keys_file: S2,
    dir: S,
    exchange: Exchange,
) -> OrderManager {
    let api = Coinnect::new_manager()
        .build_exchange_api(keys_file.as_ref().to_path_buf(), &exchange, true)
        .await
        .unwrap();
    let mut apis: HashMap<Exchange, Arc<dyn ExchangeApi>> = Default::default();
    apis.insert(api.exchange(), api);
    OrderManager::new(apis, &DbOptions::new(dir), "order_manager")
}

pub fn local_manager<S: AsRef<Path>>(path: S, api: Arc<dyn ExchangeApi>) -> Addr<OrderManager> {
    let mut apis: HashMap<Exchange, Arc<dyn ExchangeApi>> = Default::default();
    apis.insert(api.exchange(), api);
    let order_manager = OrderManager::new(apis, &DbOptions::new(path), "");
    OrderManager::start(order_manager)
}

fn expected_ok_status(order: &OrderDetail) -> binance::rest_model::OrderStatus {
    if order.order_type == OrderType::Limit && matches!(order.enforcement, Some(OrderEnforcement::FOK)) {
        binance::rest_model::OrderStatus::Filled
    } else if order.order_type == OrderType::Limit {
        binance::rest_model::OrderStatus::New
    } else {
        binance::rest_model::OrderStatus::Filled
    }
}

pub fn create_ok_order_mock(server: &MockServer, order: OrderDetail) -> Mock<'_> {
    let symbol = format!("{}{}", order.base_asset, order.quote_asset);
    register_pair_default(Exchange::Binance, &symbol, &order.pair);
    let price = order.price.unwrap_or_else(random);
    let qty = order.base_qty.unwrap_or_else(random);
    let quote_qty = price * qty;
    let response = binance::rest_model::Transaction {
        symbol,
        order_id: random(),
        client_order_id: order.id.clone(),
        transact_time: Utc::now().timestamp() as u64,
        price,
        orig_qty: qty,
        executed_qty: qty,
        cummulative_quote_qty: quote_qty,
        status: expected_ok_status(&order),
        time_in_force: order.enforcement.map_into().unwrap_or(TimeInForce::FOK),
        order_type: order.order_type.into(),
        side: order.side.into(),
        fills: vec![
            binance::rest_model::Fill {
                price: price * 0.99,
                qty: qty * 0.8,
                commission: quote_qty * Exchange::Binance.default_fees(),
                commission_asset: "USDT".to_string(),
            },
            binance::rest_model::Fill {
                price: price * 0.98,
                qty: qty * 0.2,
                commission: quote_qty * Exchange::Binance.default_fees(),
                commission_asset: "USDT".to_string(),
            },
        ],
    };
    server.mock(|when, then| {
        when.method(POST).path("/api/v3/order");
        then.status(200)
            .header("content-type", "application/json; charset=UTF-8")
            .body(serde_json::to_string(&response).unwrap());
    })
}

pub fn create_ok_margin_order_mock(server: &MockServer, order: OrderDetail) -> Mock<'_> {
    let symbol = format!("{}{}", order.base_asset, order.quote_asset);
    register_pair_default(Exchange::Binance, &symbol, &order.pair);
    let price = order.price.unwrap_or_else(random);
    let qty = order.base_qty.unwrap_or_else(random);
    let quote_qty = price * qty;
    let response = binance::rest_model::MarginOrderResult {
        symbol,
        order_id: random(),
        client_order_id: order.id.clone(),
        transact_time: Utc::now().timestamp() as u128,
        price,
        orig_qty: qty,
        executed_qty: qty,
        status: expected_ok_status(&order),
        time_in_force: order.enforcement.map_into().unwrap_or(TimeInForce::FOK),
        order_type: order.order_type.into(),
        side: order.side.into(),
        margin_buy_borrow_amount: Some(qty * 0.2),
        margin_buy_borrow_asset: Some(if order.side == TradeType::Sell {
            order.base_asset
        } else {
            order.quote_asset
        }),
        is_isolated: None,
        fills: vec![
            binance::rest_model::Fill {
                price: price * 0.99,
                qty: qty * 0.8,
                commission: quote_qty * Exchange::Binance.default_fees(),
                commission_asset: "USDT".to_string(),
            },
            binance::rest_model::Fill {
                price: price * 0.98,
                qty: qty * 0.2,
                commission: quote_qty * Exchange::Binance.default_fees(),
                commission_asset: "USDT".to_string(),
            },
        ],
        cummulative_quote_qty: 0.0,
    };
    server.mock(|when, then| {
        when.method(POST).path("/sapi/v1/margin/order");
        then.status(200)
            .header("content-type", "application/json; charset=UTF-8")
            .body(serde_json::to_string(&response).unwrap());
    })
}

pub fn new_mock_manager<S: AsRef<Path>>(path: S) -> OrderManager {
    let api: Arc<dyn ExchangeApi> = Arc::new(MockExchangeApi::default());
    let mut apis: HashMap<Exchange, Arc<dyn ExchangeApi>> = Default::default();
    apis.insert(api.exchange(), api);
    OrderManager::new(apis, &DbOptions::new(path), "")
}

pub fn mock_manager<S: AsRef<Path>>(path: S) -> Addr<OrderManager> {
    let order_manager = new_mock_manager(path);
    let act = OrderManager::start(order_manager);
    loop {
        if act.connected() {
            break;
        }
    }
    act
}

pub fn mock_manager_client<S: AsRef<Path>>(path: S) -> OrderManagerClient {
    let om = mock_manager(path);
    OrderManagerClient::new(om)
}
