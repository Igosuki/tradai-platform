use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, TimeZone, Utc};
use itertools::Itertools;
use tokio::time::Duration;

use coinnect_rt::prelude::*;
use db::DbOptions;
use stats::indicators::macd_apo::MACDApo;
use trading::book::BookPosition;
use trading::interest::test_util::mock_interest_rate_client;
use trading::order_manager::test_util::mock_manager_client;
use trading::types::OrderMode;
use util::test::test_results_dir;

use crate::driver::StrategyDriver;
use crate::mean_reverting::model::ema_indicator_model;
use crate::mean_reverting::options::Options;
use crate::mean_reverting::state::{MeanRevertingState, Operation};
use crate::mean_reverting::MeanRevertingStrategy;
use crate::test_util::draw::{draw_line_plot, StrategyEntry, TimedEntry};
use crate::test_util::fs::copy_file;
use crate::test_util::input;
use crate::test_util::{init, test_db};
use crate::types::{OperationEvent, TradeEvent};
use crate::Model;

#[derive(Debug, Serialize, Clone)]
struct StrategyLog {
    time: DateTime<Utc>,
    mid: f64,
    threshold_short: f64,
    threshold_long: f64,
    apo: f64,
    pnl: f64,
    position_return: f64,
    nominal_position: f64,
    value: MACDApo,
}

impl StrategyLog {
    fn from_state(
        time: DateTime<Utc>,
        state: &MeanRevertingState,
        ob: &Orderbook,
        value: MACDApo,
        thresholds: (f64, f64),
    ) -> StrategyLog {
        StrategyLog {
            time,
            mid: ob.avg_price().unwrap(),
            threshold_short: thresholds.0,
            threshold_long: thresholds.1,
            apo: value.apo,
            pnl: state.pnl(),
            position_return: state.position_return(),
            value,
            nominal_position: state.nominal_position(),
        }
    }
}

impl TimedEntry for StrategyLog {
    fn time(&self) -> DateTime<Utc> { self.time }
}

static EXCHANGE: &str = "Binance";
static CHANNEL: &str = "order_books";
static PAIR: &str = "BTC_USDT";

#[tokio::test]
async fn moving_average_model_backtest() {
    init();
    let db = test_db();
    let mut model = ema_indicator_model(PAIR, db, 100, 1000);
    let csv_records =
        input::load_csv_records(Utc.ymd(2021, 8, 1), Utc.ymd(2021, 8, 9), vec![PAIR], EXCHANGE, CHANNEL).await;
    csv_records[0].iter().take(500).for_each(|l| {
        let pos: BookPosition = l.to_bp();
        model.update(pos.mid).unwrap();
    });
    let apo = model.value().unwrap().apo;
    assert!(apo != 0.0, "apo should not be 0, was: {}", apo);
}

#[actix::test]
async fn spot_backtest() {
    let conf = Options::new_test_default(PAIR, Exchange::Binance);
    let positions = complete_backtest("spot", &conf).await;
    let last_position = positions.last();
    assert!(last_position.is_some(), "No position found in operations");
    assert_eq!(
        Some("44015.99".to_string()),
        last_position.map(|p| format!("{:.2}", p.pos.price))
    );
    assert_eq!(
        Some("87.87".to_string()),
        last_position.map(|p| format!("{:.2}", p.value()))
    );
}

#[actix::test]
async fn margin_backtest() {
    let conf = Options {
        order_mode: Some(OrderMode::Market),
        execution_instruction: None,
        order_asset_type: Some(AssetType::Margin),
        ..Options::new_test_default(PAIR, Exchange::Binance)
    };
    let positions = complete_backtest("margin", &conf).await;
    let last_position = positions.last();
    assert!(last_position.is_some(), "No position found in operations");
    assert_eq!(
        Some("44015.99".to_string()),
        last_position.map(|p| format!("{:.2}", p.pos.price))
    );
    assert_eq!(
        Some("83.27".to_string()),
        last_position.map(|p| format!("{:.2}", p.value()))
    );
}

async fn complete_backtest(test_name: &str, conf: &Options) -> Vec<Operation> {
    init();
    //setup_opentelemetry();
    let path = util::test::test_dir();
    let order_manager_addr = Arc::new(mock_manager_client(&path));
    let margin_interest_rate_provider_addr = Arc::new(mock_interest_rate_client(Exchange::Binance));
    let full_test_name = format!("{}_{}", module_path!(), test_name);
    let test_results_dir = test_results_dir(&full_test_name);

    let mut strat = MeanRevertingStrategy::new(
        &DbOptions::new(path),
        0.001,
        conf,
        order_manager_addr,
        margin_interest_rate_provider_addr,
        None,
    );
    let mut elapsed = 0_u128;
    let csv_records =
        input::load_csv_records(Utc.ymd(2021, 8, 1), Utc.ymd(2021, 8, 9), vec![PAIR], EXCHANGE, CHANNEL).await;
    let num_records = csv_records[0].len();
    // align data
    let pair_csv_records = csv_records[0].iter();
    let mut strategy_logs: Vec<StrategyLog> = Vec::new();
    let mut model_values: Vec<(DateTime<Utc>, MACDApo, f64)> = Vec::new();
    let mut trade_events: Vec<(OperationEvent, TradeEvent)> = Vec::new();

    let before_evals = Instant::now();
    for row in pair_csv_records.map(|csvr| {
        MarketEventEnvelope::new(
            Exchange::from(EXCHANGE.to_string()),
            PAIR.into(),
            MarketEvent::Orderbook(csvr.to_orderbook(PAIR)),
        )
    }) {
        let now = Instant::now();
        util::time::set_current_time(row.e.time());
        strat.add_event(&row).await.unwrap();
        let mut tries = 0;
        loop {
            if tries > 5 {
                break;
            }
            if strat.state.ongoing_op().is_some() {
                strat.resolve_orders().await;
                tokio::time::sleep(Duration::from_millis(10)).await;
                tries += 1;
            } else {
                break;
            }
        }
        let value = strat.model_value().unwrap();
        let row_time = row.e.time();
        model_values.push((row_time, value.clone(), strat.state.value_strat()));
        if let MarketEvent::Orderbook(ob) = row.e {
            strategy_logs.push(StrategyLog::from_state(
                row_time,
                &strat.state,
                &ob,
                value,
                strat.model.thresholds(),
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
    for op in strat.get_operations().iter().sorted_by_key(|o| o.pos.time) {
        trade_events.push((op.operation_event().clone(), op.trade_event()))
    }
    write_ema_values(&test_results_dir, &model_values);
    crate::test_util::log::write_trade_events(&test_results_dir, &trade_events);
    write_thresholds(&test_results_dir, &strategy_logs);

    // Output SVG graphs
    let draw_entries: Vec<StrategyEntry<'_, StrategyLog>> = vec![
        ("Prices and EMA", vec![|x| x.mid, |x| x.value.short_ema.current, |x| {
            x.value.long_ema.current
        }]),
        ("Open Position Return", vec![|x| x.position_return]),
        ("APO", vec![|x| x.apo]),
        ("PnL", vec![|x| x.pnl]),
        ("Nominal (units)", vec![|x| x.nominal_position]),
    ];
    let out_file = draw_line_plot(strategy_logs, draw_entries).expect("Should have drawn plots from strategy logs");
    copy_file(
        &out_file,
        &format!("{}/mean_reverting_plot_{}_latest.html", &test_results_dir, test_name),
    );

    // Find that latest operations are correct
    let mut positions = strat.get_operations();
    positions.sort_by(|p1, p2| p1.pos.time.cmp(&p2.pos.time));
    insta::assert_debug_snapshot!(positions.last());
    positions
}

fn write_ema_values(test_results_dir: &str, model_values: &[(DateTime<Utc>, MACDApo, f64)]) {
    crate::test_util::log::write_csv(
        format!("{}/{}_ema_values.csv", test_results_dir, PAIR),
        &["ts", "short_ema", "long_ema", "apo", "value_strat"],
        model_values.iter().map(|r| {
            vec![
                r.0.format(util::time::TIMESTAMP_FORMAT).to_string(),
                r.1.short_ema.current.to_string(),
                r.1.long_ema.current.to_string(),
                r.1.apo.to_string(),
                r.2.to_string(),
            ]
        }),
    )
}

fn write_thresholds(test_results_dir: &str, strategy_logs: &[StrategyLog]) {
    crate::test_util::log::write_csv(
        format!("{}/{}_thresholds.csv", test_results_dir, PAIR),
        &["ts", "threshold_short", "threshold_long"],
        strategy_logs.iter().map(|l| {
            vec![
                l.time.format(util::time::TIMESTAMP_FORMAT).to_string(),
                l.threshold_short.to_string(),
                l.threshold_long.to_string(),
            ]
        }),
    );
}
