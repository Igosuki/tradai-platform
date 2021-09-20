use std::io;
use std::sync::Arc;

use actix_http::ws;
use bytestring::ByteString;
use chrono::Utc;
use futures::Future;
use httpmock::prelude::*;
use httpmock::{Mock, MockServer};
use rand::random;

use binance::config::Config;
use binance::rest_model::TimeInForce;
use coinnect_rt::binance::BinanceApi;
use coinnect_rt::credential::{BasicCredentials, Credentials};
use coinnect_rt::exchange::{Exchange, ExchangeApi};
use coinnect_rt::pair::{default_pair_registry, PairConf};
use coinnect_rt::types::{OrderEnforcement, TradeType};
use ext::MapInto;

use crate::coinnect_types::OrderType;
use crate::order_types::OrderDetail;

type WSResponse = impl Future<Output = Result<ws::Message, io::Error>>;

type WSEndpoint = impl Fn(ws::Frame) -> WSResponse + Clone;

pub fn account_ws() -> Box<WSEndpoint> {
    let _responses_vec = vec!["hello".to_string()];
    // Create a channel to receive the events.
    let closure = async move |req: ws::Frame| unsafe {
        // let input = input.to_owned();
        // let msg = input.recv().unwrap();
        let result: Result<ws::Message, io::Error> = match req {
            ws::Frame::Ping(msg) => Ok(ws::Message::Pong(msg)),
            ws::Frame::Text(text) => Ok(ws::Message::Text(ByteString::from_bytes_unchecked(text))),
            ws::Frame::Binary(bin) => Ok(ws::Message::Binary(bin)),
            ws::Frame::Close(reason) => Ok(ws::Message::Close(reason)),
            _ => Ok(ws::Message::Close(None)),
        };
        result
    };
    Box::new(closure)
}

pub async fn local_api() -> (MockServer, Arc<dyn ExchangeApi>) {
    let server = MockServer::start();
    let creds: Box<dyn Credentials> = Box::new(BasicCredentials::empty(Exchange::Binance));
    let mock_server_address = server.address().to_string();
    let conf = Config::default()
        .set_rest_api_endpoint(&format!("http://{}", mock_server_address))
        .set_ws_endpoint(&format!("ws://{}", mock_server_address));
    let api = BinanceApi::new_with_config(creds.as_ref(), conf).await.unwrap();
    (server, Arc::new(api))
}

pub fn register_pair(symbol: &str, pair: &str) {
    let (base, quote) = pair.split_once('_').unwrap();
    let conf = PairConf {
        symbol: symbol.into(),
        pair: pair.into(),
        base: base.to_string(),
        quote: quote.to_string(),
        ..PairConf::default()
    };
    default_pair_registry().register(Exchange::Binance, vec![conf]);
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
    register_pair(&symbol, &order.pair);
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
                commission: quote_qty * 0.001,
                commission_asset: "USDT".to_string(),
            },
            binance::rest_model::Fill {
                price: price * 0.98,
                qty: qty * 0.2,
                commission: quote_qty * 0.001,
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
    register_pair(&symbol, &order.pair);
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
                commission: quote_qty * 0.001,
                commission_asset: "USDT".to_string(),
            },
            binance::rest_model::Fill {
                price: price * 0.98,
                qty: qty * 0.2,
                commission: quote_qty * 0.001,
                commission_asset: "USDT".to_string(),
            },
        ],
        cumulative_quote_qty: 0.0,
    };
    server.mock(|when, then| {
        when.method(POST).path("/sapi/v1/margin/order");
        then.status(200)
            .header("content-type", "application/json; charset=UTF-8")
            .body(serde_json::to_string(&response).unwrap());
    })
}
