use actix::Addr;
use httpmock::{Mock, MockServer};
use std::time::Duration;
use uuid::Uuid;

use super::error::*;
use super::test_util::{create_ok_margin_order_mock, create_ok_order_mock};
use crate::order_manager::test_util::{init, it_order_manager, new_mock_manager};
use crate::order_manager::types::OrderId;
use crate::order_manager::OrderManager;
use broker_test_util::binance::{account_ws as binance_account_ws, local_api};
use brokers::prelude::*;
use brokers::types::{MarginSideEffect, OrderSubmission, OrderUpdate};
use util::test::test_dir;

use super::types::{OrderDetail, OrderStatus, Rejection, StagedOrder, TransactionStatus};

#[actix::test]
async fn test_append_rejected() {
    let test_dir = test_dir();
    let mut order_manager = new_mock_manager(test_dir);
    let registered = order_manager
        .register(
            "id".to_string(),
            TransactionStatus::Rejected(Rejection::BadRequest("bad request".to_string())),
        )
        .await;
    assert!(registered.is_ok(), "{:?}", registered);
}

fn test_keys() -> String { "../config/keys_real_test.json".to_string() }

fn test_pair() -> String { "BTC_USDT".to_string() }

brokers::inventory::submit! {
    brokers::plugin::BrokerPlugin::new(Exchange::Binance, brokers::broker_binance::provide_connector)
}

#[actix::test]
async fn test_binance_stage_order_invalid() {
    let test_dir = util::test::test_dir();
    let mut order_manager = it_order_manager(test_keys(), test_dir, Exchange::Binance).await;
    let registered = order_manager
        .stage_order(StagedOrder {
            request: AddOrderRequest {
                pair: test_pair().into(),
                price: Some(0.0),
                dry_run: true,
                quantity: Some(0.0),
                side: TradeType::Buy,
                ..AddOrderRequest::default()
            },
        })
        .await;
    assert!(registered.is_ok(), "{:?}", registered);
}

#[actix::test]
async fn test_register_transactions() {
    let test_dir = util::test::test_dir();
    let mut order_manager = it_order_manager(test_keys(), test_dir, Exchange::Binance).await;
    let order_id = "1".to_string();
    let pair: Pair = "BTC_USDT".into();
    let statuses = vec![
        TransactionStatus::New(OrderSubmission {
            pair: pair.clone(),
            ..OrderSubmission::default()
        }),
        TransactionStatus::Staged(OrderQuery::AddOrder(AddOrderRequest {
            pair,
            order_id: order_id.clone(),
            ..AddOrderRequest::default()
        })),
        TransactionStatus::Filled(OrderUpdate {
            symbol: "BTCUSDT".to_string(),
            ..OrderUpdate::default()
        }),
        TransactionStatus::Rejected(Rejection::Other("".to_string())),
    ];
    // Register each status in order
    for status in &statuses {
        let reg = tokio::time::timeout(
            Duration::from_millis(50),
            order_manager.register(order_id.clone(), status.clone()),
        )
        .await;
        assert!(reg.is_ok(), "{:?}", reg);
    }
    // Get the transactions log
    let transactions = order_manager.transactions(None);
    assert!(transactions.is_ok(), "{:?}", transactions);
    itertools::assert_equal(
        transactions.unwrap().into_iter().map(|tr| (tr.id, tr.status)),
        statuses.clone().into_iter().map(|status| (order_id.clone(), status)),
    );
    // The last status for this id should be the last registered status
    let order = order_manager.get_order(order_id.clone()).await;
    pretty_assertions::assert_eq!(
        &order.unwrap(),
        statuses.last().unwrap(),
        "latest order should the last in statuses"
    );
    // Insert a new order status
    let reg = order_manager
        .register(
            order_id.clone(),
            TransactionStatus::New(OrderSubmission {
                timestamp: 0,
                id: order_id.clone(),
                ..OrderSubmission::default()
            }),
        )
        .await;
    assert!(reg.is_ok());
    let order = order_manager.get_order(order_id.clone()).await;
    // The order registry should remain unchanged
    pretty_assertions::assert_eq!(
        &order.unwrap(),
        statuses.last().unwrap(),
        "latest order should the last in statuses after registering a new order"
    );
    let compacted = order_manager.transactions_wal.get_all_compacted();
    assert!(compacted.is_ok());
    pretty_assertions::assert_eq!(
        compacted.unwrap().get(&order_id.clone()),
        statuses.last(),
        "Compacted record should be the highest inserted status"
    );
}

async fn pass_spot_order_and_expect_status(
    om: Addr<OrderManager>,
    server: &MockServer,
    request: AddOrderRequest,
    expected: OrderStatus,
) -> Result<()> {
    let staged_detail = OrderDetail::from_query(request.clone());
    let mocked_pass_order = create_ok_order_mock(server, &staged_detail);
    pass_mock_order_and_expect_status(om, mocked_pass_order, request, expected).await
}

async fn pass_mock_order_and_expect_status(
    om: Addr<OrderManager>,
    mock: Mock<'_>,
    request: AddOrderRequest,
    expected: OrderStatus,
) -> Result<()> {
    let order_detail = om
        .send(StagedOrder { request })
        .await
        .map_err(|_| Error::OrderManagerMailboxError)??;
    assert_eq!(order_detail.status, OrderStatus::Staged);
    assert_ne!(order_detail.id, "".to_string());
    assert!(!matches!(order_detail.status, OrderStatus::Rejected));
    // Wait for order to pass
    let order_id = order_detail.id.clone();
    loop {
        let (order_detail, _) = om
            .clone()
            .send(OrderId(order_id.clone()))
            .await
            .map_err(|_| Error::OrderManagerMailboxError)?;
        let order_detail = order_detail?;
        if order_detail.status != OrderStatus::Staged {
            assert_eq!(order_detail.status, expected, "{:?}", order_detail);
            mock.assert();
            break;
        }
    }
    Ok(())
}

#[actix::test]
async fn test_limit_order_workflow() -> Result<()> {
    init();
    let _account_server = broker_test_util::http::ws_it_server(binance_account_ws()).await;
    let (server, binance_api) = local_api().await;
    let test_dir = util::test::test_dir();
    let om = crate::order_manager::test_util::local_manager(test_dir, binance_api);

    let pair: Pair = "BTC_USDT".to_string().into();
    let request = AddOrderRequest {
        pair,
        price: Some(0.1),
        dry_run: false,
        quantity: Some(0.1),
        side: TradeType::Buy,
        order_id: Uuid::new_v4().to_string(),
        order_type: OrderType::Limit,
        enforcement: Some(OrderEnforcement::GTC),
        ..AddOrderRequest::default()
    };
    pass_spot_order_and_expect_status(om, &server, request, OrderStatus::Created).await
}

#[actix::test]
async fn test_market_order_workflow() -> Result<()> {
    init();
    let _account_server = broker_test_util::http::ws_it_server(binance_account_ws()).await;
    let (server, binance_api) = local_api().await;
    let test_dir = util::test::test_dir();
    let om = crate::order_manager::test_util::local_manager(test_dir, binance_api);

    let pair: Pair = "BTC_USDT".to_string().into();
    let request = AddOrderRequest {
        pair,
        dry_run: false,
        quantity: Some(0.1),
        side: TradeType::Buy,
        order_id: Uuid::new_v4().to_string(),
        order_type: OrderType::Market,
        enforcement: Some(OrderEnforcement::FOK),
        ..AddOrderRequest::default()
    };
    pass_spot_order_and_expect_status(om, &server, request, OrderStatus::Filled).await
}

#[actix::test]
async fn test_market_margin_order_workflow() -> Result<()> {
    init();
    let _account_server = broker_test_util::http::ws_it_server(binance_account_ws()).await;
    let (server, binance_api) = local_api().await;
    let test_dir = util::test::test_dir();
    let om = crate::order_manager::test_util::local_manager(test_dir, binance_api);

    let pair: Pair = "BTC_USDT".to_string().into();
    let request = AddOrderRequest {
        pair,
        dry_run: false,
        quantity: Some(0.1),
        side: TradeType::Buy,
        order_id: Uuid::new_v4().to_string(),
        order_type: OrderType::Market,
        enforcement: Some(OrderEnforcement::FOK),
        asset_type: Some(AssetType::Margin),
        side_effect_type: Some(MarginSideEffect::MarginBuy),
        ..AddOrderRequest::default()
    };
    let staged_detail = OrderDetail::from_query(request.clone());
    let mocked_pass_order = create_ok_margin_order_mock(&server, staged_detail);
    pass_mock_order_and_expect_status(om, mocked_pass_order, request, OrderStatus::Filled).await
}

#[cfg(any(feature = "live_e2e_tests", feature = "manual_e2e_tests"))]
async fn pass_live_order(om: Addr<OrderManager>, request: AddOrderRequest) -> Result<OrderDetail> {
    let order_detail = om
        .send(StagedOrder { request })
        .await
        .map_err(|_| Error::OrderManagerMailboxError)??;
    assert_eq!(order_detail.status, OrderStatus::Staged);
    assert_ne!(order_detail.id, "".to_string());
    assert!(!matches!(order_detail.status, OrderStatus::Rejected));
    // Wait for order to pass
    let order_id = order_detail.id.clone();
    loop {
        let (order_detail, _) = om
            .clone()
            .send(OrderId(order_id.clone()))
            .await
            .map_err(|_| Error::OrderManagerMailboxError)?;
        let order_detail = order_detail?;
        if order_detail.status != OrderStatus::Staged {
            return Ok(order_detail);
        }
    }
}

#[cfg(feature = "live_e2e_tests")]
#[actix::test]
async fn test_live_market_margin_order_workflow() -> Result<()> {
    use brokers::{coinnect::Coinnect, types::AccountType};

    init();
    let test_dir = util::test::e2e_test_dir();
    // Build a valid test engine
    let (credentials, apis) = crate::test_util::e2e::build_apis().await?;
    let om = crate::order_manager::test_util::local_manager(test_dir, apis.get(&Exchange::Binance).unwrap().clone());
    let _account_stream = brokers::coinnect::Coinnect::new_account_stream(
        Exchange::Binance,
        credentials,
        vec![om.clone().recipient()],
        false,
        AccountType::Margin,
    )
    .await?;
    Coinnect::load_pair_registries(apis.clone()).await?;
    // Scenario based on trading BTC vs USDT on a margin account
    // 1- LONG : Buy the minimal BTC amount, then sell it without side effect
    // 1a - check that there is no borrowed amount
    // 2- SHORT : Sell the minimal BTC amount, then buy it with MARGIN_BUY and AUTO_REPAY
    // 2a - check that there is a borrowed amount
    let pair: Pair = "BTC_USDT".into();
    let base_margin_order = AddOrderRequest {
        pair: pair.clone(),
        dry_run: false,
        quantity: Some(0.0004),
        order_type: OrderType::Market,
        asset_type: Some(AssetType::Margin),
        ..AddOrderRequest::default()
    };
    let buy_long = AddOrderRequest {
        side: TradeType::Buy,
        side_effect_type: None,
        order_id: Some(Uuid::new_v4().to_string()),
        ..base_margin_order.clone()
    };
    let buy_long_order_detail = pass_live_order(om.clone(), buy_long).await?;
    eprintln!("buy_long_order_detail = {:?}", buy_long_order_detail);
    let sell_long = AddOrderRequest {
        side: TradeType::Sell,
        side_effect_type: None,
        order_id: Some(Uuid::new_v4().to_string()),
        ..base_margin_order.clone()
    };
    let sell_long_order_detail = pass_live_order(om.clone(), sell_long).await?;
    eprintln!("sell_long_order_detail = {:?}", sell_long_order_detail);
    let margined_qty = base_margin_order.quantity.map(|q| q * 1.2);
    let sell_short = AddOrderRequest {
        side: TradeType::Sell,
        quantity: margined_qty,
        side_effect_type: Some(MarginSideEffect::MarginBuy),
        order_id: Some(Uuid::new_v4().to_string()),
        ..base_margin_order.clone()
    };
    let sell_short_order_detail = pass_live_order(om.clone(), sell_short).await?;
    eprintln!("sell_short_order_detail = {:?}", sell_short_order_detail);
    let buy_short = AddOrderRequest {
        side: TradeType::Buy,
        quantity: margined_qty,
        side_effect_type: Some(MarginSideEffect::AutoRepay),
        order_id: Some(Uuid::new_v4().to_string()),
        ..base_margin_order.clone()
    };
    let buy_short_order_detail = pass_live_order(om.clone(), buy_short).await?;
    eprintln!("buy_short_order_detail = {:?}", buy_short_order_detail);

    Ok(())
}

#[cfg(feature = "manual_e2e_tests")]
#[actix::test]
async fn test_live() -> Result<()> {
    init();
    let test_dir = util::test::e2e_test_dir();
    // Build a valid test engine
    let (_credentials, apis) = crate::test_util::e2e::build_apis().await?;
    let om = crate::order_manager::test_util::local_manager(test_dir, apis.get(&Exchange::Binance).unwrap().clone());
    // let _account_stream = brokers::coinnect::Coinnect::new_account_stream(
    //     Exchange::Binance,
    //     credentials,
    //     vec![om.clone().recipient()],
    //     false,
    //     AccountType::Margin,
    // )
    // .await?;
    Coinnect::load_pair_registries(apis.clone()).await?;
    let pair: Pair = "ETC_USDT".into();
    let qty = 0.2 + 0.00002084;
    //let qty = 1.0;
    let base_margin_order = AddOrderRequest {
        pair: pair.clone(),
        dry_run: false,
        quantity: Some(qty),
        order_type: OrderType::Market,
        asset_type: Some(AssetType::IsolatedMargin),
        ..AddOrderRequest::default()
    };
    let buy_long = AddOrderRequest {
        side: TradeType::Buy,
        side_effect_type: Some(MarginSideEffect::AutoRepay),
        order_id: AddOrderRequest::new_id(),
        ..base_margin_order.clone()
    };
    let order_detail = pass_live_order(om.clone(), buy_long).await?;
    eprintln!("order_detail = {:?}", order_detail);
    Ok(())
}
