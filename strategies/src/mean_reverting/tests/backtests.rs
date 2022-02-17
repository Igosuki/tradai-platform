use std::collections::HashMap;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use serde_json::Value;

use brokers::prelude::*;
use strategy::driver::{StratProviderRef, StrategyInitContext};
use strategy_test_util::draw::StrategyEntry;
use strategy_test_util::it_backtest::{generic_backtest, BacktestRange};
use strategy_test_util::log::StrategyLog;
use trading::order_manager::types::OrderDetail;
use trading::types::{OrderConf, OrderMode};

use crate::mean_reverting::options::Options;
use crate::mean_reverting::MeanRevertingStrategy;

static EXCHANGE: &str = "Binance";
static PAIR: &str = "BTC_USDT";

fn get_f64(model: &HashMap<String, Option<Value>>, key: &str) -> f64 {
    model.get(key).unwrap().as_ref().unwrap().as_f64().unwrap()
}

lazy_static! {
    static ref MEAN_REVERTING_DRAW_ENTRIES: Vec<StrategyEntry<StrategyLog, &'static str>> = {
        vec![
            (
                "Prices and EMA",
                Arc::new(|x| {
                    vec![
                        (
                            "mid_price",
                            *x.prices
                                .get(&(EXCHANGE.to_string(), PAIR.to_string()))
                                .unwrap_or(&f64::NAN),
                        ),
                        ("short_ema", get_f64(&x.model, "short_ema")),
                        ("long_ema", get_f64(&x.model, "long_ema")),
                    ]
                }),
            ),
            ("APO", Arc::new(|x| vec![("apo", get_f64(&x.model, "apo"))])),
            (
                "Portfolio Return",
                Arc::new(|x| vec![("pfl_return", x.snapshot.current_return)]),
            ),
            ("Portfolio PnL", Arc::new(|x| vec![("pnl", x.snapshot.pnl)])),
            ("Portfolio Value", Arc::new(|x| vec![("value", x.snapshot.value)])),
            (
                "Nominal (units)",
                Arc::new(|x| {
                    vec![(
                        "nominal",
                        *x.nominal_positions
                            .get(&(EXCHANGE.to_string(), PAIR.to_string()))
                            .unwrap_or(&f64::NAN),
                    )]
                }),
            ),
        ]
    };
}

#[actix::test]
async fn spot_backtest() {
    let provider: StratProviderRef = Arc::new(|ctx: StrategyInitContext| {
        let exchange = Exchange::Binance;
        let conf = Options::new_test_default(PAIR, exchange);
        Box::new(MeanRevertingStrategy::new(
            ctx.db,
            "mean_reverting_test".to_string(),
            &conf,
            None,
        ))
    });
    let exchange = Exchange::Binance;
    let full_test_name = format!("{}_{}", module_path!(), "spot");
    let entries = MEAN_REVERTING_DRAW_ENTRIES.clone();
    let positions = generic_backtest(
        &full_test_name,
        provider,
        &entries,
        &BacktestRange::new(Utc.ymd(2021, 8, 1), Utc.ymd(2021, 8, 9)),
        &[exchange],
        100.0,
        0.001,
    )
    .await;
    let last_position = positions.last();
    assert!(last_position.is_some(), "No position found in operations");
    assert_eq!(
        Some("44015.99".to_string()),
        last_position.map(|p| format!("{:.2}", p.current_symbol_price))
    );
    assert_eq!(
        Some("87.71".to_string()),
        last_position
            .and_then(|p| p.close_order.as_ref().map(OrderDetail::realized_quote_value))
            .map(|f| format!("{:.2}", f))
    );
    assert_eq!(
        Some("87.71".to_string()),
        last_position.map(|p| format!("{:.2}", p.current_value_gross()))
    );
}

#[actix::test]
async fn margin_backtest() {
    let provider: StratProviderRef = Arc::new(|ctx: StrategyInitContext| {
        let exchange = Exchange::Binance;
        let conf = Options {
            order_conf: OrderConf {
                order_mode: OrderMode::Market,
                execution_instruction: None,
                asset_type: AssetType::Margin,
                dry_mode: true,
            },
            ..Options::new_test_default(PAIR, exchange)
        };
        Box::new(MeanRevertingStrategy::new(
            ctx.db,
            "mean_reverting_test".to_string(),
            &conf,
            None,
        ))
    });
    let exchange = Exchange::Binance;
    let full_test_name = format!("{}_{}", module_path!(), "spot");
    let entries = MEAN_REVERTING_DRAW_ENTRIES.clone();
    let positions = generic_backtest(
        &full_test_name,
        provider,
        &entries,
        &BacktestRange::new(Utc.ymd(2021, 8, 1), Utc.ymd(2021, 8, 9)),
        &[exchange],
        100.0,
        0.001,
    )
    .await;

    let last_position = positions.last();
    assert!(last_position.is_some(), "No position found in operations");
    assert_eq!(
        Some("44015.99".to_string()),
        last_position.map(|p| format!("{:.2}", p.current_symbol_price))
    );
    assert_eq!(
        Some("82.47".to_string()),
        last_position.map(|p| format!("{:.2}", p.current_value_gross()))
    );
}
