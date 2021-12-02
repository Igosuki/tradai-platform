use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, TimeZone, Utc};
use itertools::Itertools;
use serde_json::Value;
use tokio::time::Duration;

use coinnect_rt::prelude::*;
use portfolio::portfolio::Portfolio;
use trading::book::BookPosition;
use trading::engine::mock_engine;
use trading::position::Position;
use trading::types::{OrderConf, OrderMode};
use util::test::test_results_dir;

use crate::driver::StrategyDriver;
use crate::event::trades_history;
use crate::generic::{GenericDriver, GenericDriverOptions, PortfolioOptions, SerializedModel, Strategy};
use crate::mean_reverting::model::ema_indicator_model;
use crate::mean_reverting::options::Options;
use crate::mean_reverting::MeanRevertingStrategy;
use crate::test_util::draw::{draw_line_plot, StrategyEntry, TimedEntry};
use crate::test_util::fs::copy_file;
use crate::test_util::{init, test_db};
use crate::test_util::{input, test_db_with_path};
use crate::{DataQuery, DataResult, Model};

#[derive(Debug, Serialize, Clone)]
struct StrategyLog {
    time: DateTime<Utc>,
    mid: f64,
    threshold_short: f64,
    threshold_long: f64,
    apo: f64,
    short_ema: f64,
    long_ema: f64,
    pnl: f64,
    position_return: f64,
    nominal_position: f64,
}

impl StrategyLog {
    fn new(
        xch: Exchange,
        pair: Pair,
        time: DateTime<Utc>,
        portfolio: &Portfolio,
        ob: &Orderbook,
        value: &SerializedModel,
    ) -> StrategyLog {
        let model: HashMap<&str, &Option<Value>> = value.iter().map(|kv| (kv.0.as_str(), &kv.1)).collect();
        let get_f64 = |key: &str| model.get(key).unwrap().as_ref().unwrap().as_f64().unwrap();
        StrategyLog {
            time,
            mid: ob.avg_price().unwrap(),
            threshold_short: get_f64("threshold_short"),
            threshold_long: get_f64("threshold_long"),
            apo: get_f64("apo"),
            short_ema: get_f64("short_ema"),
            long_ema: get_f64("long_ema"),
            pnl: portfolio.pnl(),
            position_return: portfolio.current_return(),
            nominal_position: portfolio.open_position(xch, pair).map(|p| p.quantity).unwrap_or(0.0),
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
    csv_records[0].1.iter().take(500).for_each(|l| {
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
        last_position.map(|p| format!("{:.2}", p.current_symbol_price))
    );
    assert_eq!(
        Some("87.87".to_string()),
        last_position.map(|p| format!("{:.2}", p.current_value_gross()))
    );
}

#[actix::test]
async fn margin_backtest() {
    let conf = Options {
        order_conf: OrderConf {
            order_mode: OrderMode::Market,
            execution_instruction: None,
            asset_type: AssetType::Margin,
            dry_mode: true,
        },
        ..Options::new_test_default(PAIR, Exchange::Binance)
    };
    let positions = complete_backtest("margin", &conf).await;
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

async fn complete_backtest(test_name: &str, conf: &Options) -> Vec<Position> {
    init();
    //setup_opentelemetry();
    let path = util::test::test_dir();
    let engine = Arc::new(mock_engine(path.path(), &[Exchange::Binance]));
    let full_test_name = format!("{}_{}", module_path!(), test_name);
    let test_results_dir = test_results_dir(&full_test_name);
    let db = test_db_with_path(path);
    let fees_rate = 0.001;
    let embedded = MeanRevertingStrategy::new(
        db.clone(),
        "mean_reverting_test".to_string(),
        fees_rate,
        conf,
        engine.clone(),
        None,
    );
    let generic_options = GenericDriverOptions {
        portfolio: PortfolioOptions {
            fees_rate,
            initial_quote_cash: conf.initial_cap,
        },
    };
    let mut strat = GenericDriver::try_new(
        <dyn Strategy>::channels(&embedded),
        db,
        &generic_options,
        Box::new(embedded),
        engine,
    )
    .unwrap();
    let mut elapsed = 0_u128;
    let events = input::load_csv_events(Utc.ymd(2021, 8, 1), Utc.ymd(2021, 8, 9), vec![PAIR], EXCHANGE, CHANNEL).await;
    let (events, count) = events.tee();
    let num_records = count.count();
    // align data
    let mut strategy_logs: Vec<StrategyLog> = Vec::new();
    let mut model_values: Vec<(DateTime<Utc>, SerializedModel, f64)> = Vec::new();

    let exchange = Exchange::from(EXCHANGE.to_string());
    let pair: Pair = PAIR.into();
    let before_evals = Instant::now();
    for event in events {
        let now = Instant::now();
        let event_time = event.e.time();
        util::time::set_current_time(event_time);
        strat.add_event(&event).await.unwrap();
        let mut tries = 0;
        loop {
            if tries > 5 {
                break;
            }
            if strat.portfolio.is_locked(&(exchange, pair.clone())) {
                strat.resolve_orders().await;
                tokio::time::sleep(Duration::from_millis(10)).await;
                tries += 1;
            } else {
                break;
            }
        }
        if let Ok(DataResult::Models(models)) = strat.data(DataQuery::Models).await {
            model_values.push((event_time, models.clone(), strat.portfolio.value()));
            if let MarketEvent::Orderbook(ob) = event.e {
                strategy_logs.push(StrategyLog::new(
                    exchange,
                    pair.clone(),
                    event_time,
                    &strat.portfolio,
                    &ob,
                    &models,
                ));
            }
        }

        elapsed += now.elapsed().as_nanos();
    }
    info!(
        "For {} records, evals took {}ms, each iteration took {} ns on avg",
        num_records,
        before_evals.elapsed().as_millis(),
        elapsed / num_records as u128
    );
    let trade_events = trades_history(&strat.portfolio);
    write_models(&test_results_dir, &model_values);
    crate::test_util::log::write_trade_events(&test_results_dir, &trade_events);

    // Output SVG graphs
    let draw_entries: Vec<StrategyEntry<'_, StrategyLog>> = vec![
        ("Prices and EMA", vec![|x| x.mid, |x| x.short_ema, |x| x.long_ema]),
        ("Open Position Return", vec![|x| x.position_return]),
        ("APO", vec![|x| x.apo]),
        ("PnL", vec![|x| x.pnl]),
        ("Nominal (units)", vec![|x| x.nominal_position]),
    ];
    let out_file = draw_line_plot(module_path!(), strategy_logs, draw_entries)
        .expect("Should have drawn plots from strategy logs");
    copy_file(
        &out_file,
        &format!("{}/mean_reverting_plot_{}_latest.html", &test_results_dir, test_name),
    );

    let mut positions = strat.portfolio.positions_history().unwrap();
    positions.sort_by(|p1, p2| p1.meta.close_at.cmp(&p2.meta.close_at));
    //insta::assert_debug_snapshot!(positions.last());
    positions
}

fn write_models(test_results_dir: &str, model_values: &[(DateTime<Utc>, SerializedModel, f64)]) {
    if let Some(last_model) = model_values.iter().last() {
        let model_keys = last_model.1.iter().map(|v| v.0.clone()).collect::<Vec<String>>();
        let mut csv_keys = vec!["ts", "value_strat"];
        csv_keys.extend(model_keys.iter().map(|s| s.as_str()));
        crate::test_util::log::write_csv(
            format!("{}/{}_ema_values.csv", test_results_dir, PAIR),
            &csv_keys,
            model_values.iter().map(|r| {
                let mut values = vec![r.0.format(util::time::TIMESTAMP_FORMAT).to_string(), r.2.to_string()];
                for (_, model_value) in &r.1 {
                    values.push(model_value.as_ref().unwrap_or(&Value::Null).to_string());
                }
                values
            }),
        )
    } else {
        error!("no model values to write");
    }
}
