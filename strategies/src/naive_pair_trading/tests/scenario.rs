use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Duration, TimeZone, Utc};
use itertools::Itertools;
use serde::Serialize;

use crate::driver::StrategyDriver;
use crate::event::trades_history;
use coinnect_rt::exchange::Exchange;
use coinnect_rt::types::Pair;
use portfolio::portfolio::Portfolio;
use trading::engine::mock_engine;
use trading::position::Position;
use util::test::test_results_dir;

use crate::naive_pair_trading::covar_model::{LinearModelValue, LinearSpreadModel};
use crate::naive_pair_trading::options::Options;
use crate::naive_pair_trading::{DualBookPosition, NaiveTradingStrategy};
use crate::test_util::draw::{draw_line_plot, StrategyEntry, TimedEntry};
use crate::test_util::fs::copy_file;
use crate::test_util::test_db;
use crate::test_util::{input, test_db_with_path};

static LEFT_PAIR: &str = "ETH_USDT";
static RIGHT_PAIR: &str = "BTC_USDT";

#[derive(Debug, Serialize)]
struct StrategyLog {
    time: DateTime<Utc>,
    right_mid: f64,
    left_mid: f64,
    predicted_right: f64,
    pnl: f64,
    res: f64,
    alpha: f64,
    pos_return: f64,
    beta: f64,
    left_qty: f64,
    right_qty: f64,
    left_price: f64,
    right_price: f64,
    value: f64,
}

impl StrategyLog {
    #[allow(clippy::too_many_arguments)]
    fn from_state(
        time: DateTime<Utc>,
        portfolio: &Portfolio,
        left_pos: Option<&Position>,
        right_pos: Option<&Position>,
        last_pos: &DualBookPosition,
        res: f64,
        predicted_right: f64,
        model_value: Option<LinearModelValue>,
    ) -> StrategyLog {
        StrategyLog {
            time,
            right_mid: last_pos.right.mid,
            left_mid: last_pos.left.mid,
            pos_return: portfolio.current_return(),
            pnl: portfolio.pnl(),
            beta: model_value.as_ref().map(|lm| lm.beta).unwrap_or(0.0),
            alpha: model_value.as_ref().map(|lm| lm.alpha).unwrap_or(0.0),
            predicted_right,
            res,
            left_qty: left_pos.as_ref().map(|p| p.quantity).unwrap_or(0.0),
            right_qty: right_pos.as_ref().map(|p| p.quantity).unwrap_or(0.0),
            left_price: left_pos
                .as_ref()
                .and_then(|p| p.open_order.as_ref())
                .and_then(|o| o.price)
                .unwrap_or(0.0),
            right_price: right_pos
                .as_ref()
                .and_then(|p| p.open_order.as_ref())
                .and_then(|o| o.price)
                .unwrap_or(0.0),
            value: portfolio.value(),
        }
    }
}

impl TimedEntry for StrategyLog {
    fn time(&self) -> DateTime<Utc> { self.time }
}

fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

static EXCHANGE: &str = "Binance";
static CHANNEL: &str = "order_books";

#[tokio::test]
async fn model_backtest() {
    init();
    let db = test_db();
    let mut model = LinearSpreadModel::new(db, "BTC_USDT_ETH_USDT", 500, Duration::milliseconds(200), 500);
    // Read downsampled streams
    let records = input::load_csv_records(
        Utc.ymd(2020, 3, 25),
        Utc.ymd(2020, 3, 25),
        vec![LEFT_PAIR, RIGHT_PAIR],
        EXCHANGE,
        CHANNEL,
    )
    .await;
    // align data
    records[0]
        .1
        .iter()
        .zip(records[1].1.iter())
        .take(500)
        .for_each(|(l, r)| {
            model.push(&DualBookPosition {
                time: l.event_ms,
                left: l.to_bp(),
                right: r.to_bp(),
            })
        });
    let lm = model.value().unwrap();
    assert!(lm.beta > 0.0, "beta {:?} should be positive", lm.beta);
}

#[actix::test]
async fn complete_backtest() {
    init();
    let path = util::test::test_dir();
    let exchange = Exchange::Binance;
    let engine = Arc::new(mock_engine(path.as_ref(), &[exchange]));
    let test_results_dir = test_results_dir(module_path!());
    let db = test_db_with_path(path);
    let left_pair: Pair = LEFT_PAIR.into();
    let right_pair: Pair = RIGHT_PAIR.into();
    let mut strat = NaiveTradingStrategy::new(
        db,
        "naive_strat_test".to_string(),
        0.001,
        &Options {
            left: left_pair.clone(),
            right: right_pair.clone(),
            beta_eval_freq: 1000,
            beta_sample_freq: "1min".to_string(),
            window_size: 2000,
            exchange,
            threshold_long: -0.03,
            threshold_short: 0.03,
            stop_loss: -0.1,
            stop_gain: 0.075,
            initial_cap: 100.0,
            order_conf: Default::default(),
        },
        engine,
        None,
    );
    // Read downsampled streams
    let mut elapsed = 0_u128;
    let events = input::load_csv_events(
        Utc.ymd(2020, 3, 25),
        Utc.ymd(2020, 4, 8),
        vec![LEFT_PAIR, RIGHT_PAIR],
        EXCHANGE,
        CHANNEL,
    )
    .await;
    let (count, events) = events.tee();
    let mut logs: Vec<StrategyLog> = Vec::new();
    let mut model_values: Vec<(DateTime<Utc>, LinearModelValue, f64, f64, f64)> = Vec::new();
    let before_evals = Instant::now();
    let num_records = count.count();
    for event in events {
        let now = Instant::now();
        util::time::set_current_time(event.e.time());
        strat.add_event(&event).await.unwrap();
        let mut tries = 0;
        loop {
            if tries > 5 {
                break;
            }
            if strat.portfolio.is_locked(&(exchange, left_pair.clone()))
                || strat.portfolio.is_locked(&(exchange, right_pair.clone()))
            {
                strat.resolve_orders().await;
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                tries += 1;
            } else {
                break;
            }
        }
        if let Some(dual_bp) = strat.last_dual_bp() {
            let res = strat.calc_pred_ratio(&dual_bp).unwrap_or(0.0);
            let predicted_right = strat.predict_right(&dual_bp).unwrap_or(0.0);
            if let Some(value) = strat.model_value() {
                model_values.push((event.ts, value.clone(), predicted_right, res, strat.portfolio.value()));
            }
            logs.push(StrategyLog::from_state(
                event.ts,
                &strat.portfolio,
                strat.portfolio.open_position(exchange, left_pair.clone()),
                strat.portfolio.open_position(exchange, right_pair.clone()),
                &dual_bp,
                res,
                predicted_right,
                strat.model_value(),
            ));
        }
        elapsed += now.elapsed().as_nanos();
    }
    info!(
        "For {} records, evals took {}ms, each iteration took {} ns on avg",
        num_records,
        before_evals.elapsed().as_millis(),
        elapsed / num_records as u128
    );

    write_model_values(&test_results_dir, &model_values);
    let trade_events = trades_history(&strat.portfolio);
    crate::test_util::log::write_trade_events(&test_results_dir, &trade_events);

    let draw_entries: Vec<StrategyEntry<'_, StrategyLog>> = vec![
        ("value", vec![|x| x.right_mid, |x| x.predicted_right]),
        ("return", vec![|x| x.pos_return]),
        ("PnL", vec![|x| x.pnl]),
        ("Nominal Position", vec![|x| x.left_qty]),
        ("Beta", vec![|x| x.beta]),
        ("res", vec![|x| x.res]),
        ("traded_price_left", vec![|x| x.left_price]),
        ("traded_price_right", vec![|x| x.right_price]),
        ("alpha_val", vec![|x| x.alpha]),
        ("value_strat", vec![|x| x.value]),
        ("predicted_right_price", vec![|x| x.predicted_right]),
    ];
    let out_file =
        draw_line_plot(module_path!(), logs, draw_entries).expect("Should have drawn plots from strategy logs");
    copy_file(
        &out_file,
        &format!("{}/naive_pair_trading_plot_{}_latest.html", &test_results_dir, "spot"),
    );

    let mut positions = strat.portfolio.positions_history().unwrap();
    positions.sort_by(|p1, p2| p1.meta.close_at.cmp(&p2.meta.close_at));
    let left_positions = positions.iter().filter(|p| p.symbol == left_pair);
    let last_position = left_positions.last();
    assert_eq!(
        Some(162.130004882813),
        last_position.and_then(|p| p.open_order.as_ref().and_then(|o| o.price))
    );
    assert_eq!(
        Some(33.33032942489664),
        last_position.and_then(|p| p.open_order.as_ref().map(|o| o.realized_quote_value()))
    );
}

fn write_model_values(test_results_dir: &str, model_values: &[(DateTime<Utc>, LinearModelValue, f64, f64, f64)]) {
    crate::test_util::log::write_csv(
        format!("{}/model_values.csv", test_results_dir),
        &["ts", "beta", "alpha", "predicted_right", "res", "value_strat"],
        model_values.iter().map(|r| {
            vec![
                r.0.format(util::time::TIMESTAMP_FORMAT).to_string(),
                r.1.beta.to_string(),
                r.1.alpha.to_string(),
                r.2.to_string(),
                r.3.to_string(),
                r.4.to_string(),
            ]
        }),
    )
}
