use std::collections::HashMap;

use chrono::{TimeZone, Utc};
use serde_json::Value;

use strategy::coinnect::prelude::*;
use strategy_test_util::draw::StrategyEntry;
use strategy_test_util::it_backtest::{generic_backtest, BacktestRange, BacktestStratProvider, GenericTestContext};
use strategy_test_util::log::StrategyLog;
use trading::types::{OrderConf, OrderMode};

use crate::naive_pair_trading::options::Options;
use crate::naive_pair_trading::{covar_model, NaiveTradingStrategy};

static EXCHANGE: &str = "Binance";
static LEFT_PAIR: &str = "ETH_USDT";
static RIGHT_PAIR: &str = "BTC_USDT";

fn get_f64(model: &HashMap<String, Option<Value>>, key: &str) -> f64 {
    model
        .get(key)
        .and_then(|v| v.as_ref().and_then(|o| o.as_f64()))
        .unwrap_or(f64::NAN)
}

// ("traded_price_right", |x| vec![("traded_right_price", x.right_price)]),
// ("traded_price_left", |x| vec![("traded_left_price", x.left_price)]),
// ("res", |x| vec![("res", x.res)]),

lazy_static! {
    static ref NAIVE_STRATEGY_DRAW_ENTRIES: Vec<StrategyEntry<'static, StrategyLog>> = {
        vec![
            ("Right Price vs Predicted", |x| {
                let right_price = *x
                    .prices
                    .get(&(EXCHANGE.to_string(), RIGHT_PAIR.to_string()))
                    .unwrap_or(&f64::NAN);
                let left_price = *x
                    .prices
                    .get(&(EXCHANGE.to_string(), LEFT_PAIR.to_string()))
                    .unwrap_or(&f64::NAN);
                let alpha = get_f64(&x.model, "alpha");
                let beta = get_f64(&x.model, "beta");
                vec![
                    ("right_price", right_price),
                    ("predicted_right", covar_model::predict(alpha, beta, left_price)),
                    ("alpha", alpha),
                ]
            }),
            ("Alpha", |x| vec![("alpha", get_f64(&x.model, "alpha"))]),
            ("Beta", |x| vec![("beta", get_f64(&x.model, "beta"))]),
            ("Portfolio Return", |x| vec![("pfl_return", x.snapshot.current_return)]),
            ("Portfolio PnL", |x| vec![("pnl", x.snapshot.pnl)]),
            ("Portfolio Value", |x| vec![("value", x.snapshot.value)]),
            ("Left Traded Qty", |x| {
                vec![(
                    "left_qty",
                    *x.nominal_positions
                        .get(&(EXCHANGE.to_string(), LEFT_PAIR.to_string()))
                        .unwrap_or(&f64::NAN),
                )]
            }),
            ("Right Traded Qty", |x| {
                vec![(
                    "right_qty",
                    *x.nominal_positions
                        .get(&(EXCHANGE.to_string(), RIGHT_PAIR.to_string()))
                        .unwrap_or(&f64::NAN),
                )]
            }),
        ]
    };
}

#[actix::test]
async fn spot_backtest() {
    let provider: BacktestStratProvider = |ctx: GenericTestContext| {
        let exchange = Exchange::Binance;
        let conf = Options::new_test_default(exchange, LEFT_PAIR.into(), RIGHT_PAIR.into());
        Box::new(NaiveTradingStrategy::new(
            ctx.db,
            "naive_trading_test".to_string(),
            &conf,
            ctx.engine,
            None,
        ))
    };
    let exchange = Exchange::Binance;
    let full_test_name = format!("{}_{}", module_path!(), "spot");
    let positions = generic_backtest(
        &full_test_name,
        provider,
        &NAIVE_STRATEGY_DRAW_ENTRIES,
        &BacktestRange::new(Utc.ymd(2020, 3, 25), Utc.ymd(2020, 4, 8)),
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
        Some("87.87".to_string()),
        last_position
            .and_then(|p| p.close_order.as_ref().map(|o| o.realized_quote_value()))
            .map(|f| format!("{:.2}", f))
    );
    assert_eq!(
        Some("87.87".to_string()),
        last_position.map(|p| format!("{:.2}", p.current_value_gross()))
    );
}

#[actix::test]
async fn margin_backtest() {
    let provider: BacktestStratProvider = |ctx: GenericTestContext| {
        let exchange = Exchange::Binance;
        let conf = Options {
            order_conf: OrderConf {
                order_mode: OrderMode::Market,
                execution_instruction: None,
                asset_type: AssetType::Margin,
                dry_mode: true,
            },
            ..Options::new_test_default(exchange, LEFT_PAIR.into(), RIGHT_PAIR.into())
        };
        Box::new(NaiveTradingStrategy::new(
            ctx.db,
            "naive_trading_test".to_string(),
            &conf,
            ctx.engine,
            None,
        ))
    };
    let exchange = Exchange::Binance;
    let full_test_name = format!("{}_{}", module_path!(), "margin");
    let positions = generic_backtest(
        &full_test_name,
        provider,
        &NAIVE_STRATEGY_DRAW_ENTRIES,
        &BacktestRange::new(Utc.ymd(2020, 3, 25), Utc.ymd(2020, 4, 8)),
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
        Some("83.27".to_string()),
        last_position.map(|p| format!("{:.2}", p.current_value_gross()))
    );
}
