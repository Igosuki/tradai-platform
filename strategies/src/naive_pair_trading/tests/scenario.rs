use std::error::Error;
use std::time::Instant;

use chrono::{DateTime, TimeZone, Utc};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use plotters::prelude::*;
use serde::Serialize;

use coinnect_rt::exchange::Exchange;
use db::get_or_create;

use crate::input;
use crate::naive_pair_trading::covar_model::LinearModelValue;
use crate::naive_pair_trading::options::Options;
use crate::naive_pair_trading::state::MovingState;
use crate::naive_pair_trading::{covar_model, DataRow, NaiveTradingStrategy};
use crate::order_manager::test_util::mock_manager;
use crate::test_util::test_results_dir;
use crate::types::{OperationEvent, OrderMode, TradeEvent};

static LEFT_PAIR: &str = "LTC_USDT";
static RIGHT_PAIR: &str = "BTC_USDT";

#[derive(Debug, Serialize)]
struct StrategyLog {
    time: DateTime<Utc>,
    right_mid: f64,
    left_mid: f64,
    predicted_right: f64,
    short_position_return: f64,
    long_position_return: f64,
    pnl: f64,
    nominal_position: f64,
    beta_lr: f64,
    res: f64,
    traded_price_left: f64,
    traded_price_right: f64,
    alpha: f64,
    value_strat: f64,
}

impl StrategyLog {
    fn from_state(
        time: DateTime<Utc>,
        state: &MovingState,
        last_row: &DataRow,
        _model_value: Option<LinearModelValue>,
    ) -> StrategyLog {
        StrategyLog {
            time,
            right_mid: last_row.right.mid,
            left_mid: last_row.left.mid,
            predicted_right: state.predicted_right(),
            short_position_return: state.short_position_return(),
            long_position_return: state.long_position_return(),
            pnl: state.pnl(),
            nominal_position: state.nominal_position(),
            beta_lr: state.beta_lr(),
            res: state.res(),
            traded_price_left: state.traded_price_left(),
            traded_price_right: state.traded_price_right(),
            alpha: state.alpha(),
            value_strat: state.value_strat(),
        }
    }
}

type StrategyEntry<'a> = (&'a str, Vec<fn(&StrategyLog) -> f64>);

fn draw_line_plot(data: Vec<StrategyLog>) -> std::result::Result<String, Box<dyn Error>> {
    std::fs::create_dir_all("graphs").unwrap();
    let now = Utc::now();
    let out_file = format!("graphs/naive_pair_trading_plot_{}.svg", now.format("%Y%m%d%H:%M:%S"));
    let color_wheel = vec![&BLACK, &BLUE, &RED];
    let more_lines: Vec<StrategyEntry<'_>> = vec![
        ("value", vec![|x| x.right_mid, |x| x.predicted_right]),
        ("return", vec![|x| x.short_position_return + x.long_position_return]),
        ("PnL", vec![|x| x.pnl]),
        ("Nominal Position", vec![|x| x.nominal_position]),
        ("Beta", vec![|x| x.beta_lr]),
        ("res", vec![|x| x.res]),
        ("traded_price_left", vec![|x| x.traded_price_left]),
        ("traded_price_right", vec![|x| x.traded_price_right]),
        ("alpha_val", vec![|x| x.alpha]),
        ("value_strat", vec![|x| x.value_strat]),
    ];
    let height: u32 = 342 * more_lines.len() as u32;
    let root = SVGBackend::new(&out_file, (1724, height)).into_drawing_area();
    root.fill(&WHITE)?;

    let lower = data.first().unwrap().time;
    let upper = data.last().unwrap().time;
    let x_range = lower..upper;

    let area_rows = root.split_evenly((more_lines.len(), 1));

    let skipped_data = data.iter().skip(501);
    for (i, line_specs) in more_lines.iter().enumerate() {
        let mins = skipped_data.clone().map(|sl| {
            line_specs
                .1
                .iter()
                .map(|line_spec| OrderedFloat(line_spec(sl)))
                .min()
                .unwrap()
        });
        let maxs = skipped_data.clone().map(|sl| {
            line_specs
                .1
                .iter()
                .map(|line_spec| OrderedFloat(line_spec(sl)))
                .max()
                .unwrap()
        });
        let y_range = mins.min().unwrap().0..maxs.max().unwrap().0;

        let mut chart = ChartBuilder::on(&area_rows[i])
            .x_label_area_size(60)
            .y_label_area_size(60)
            .caption(line_specs.0, ("sans-serif", 50.0).into_font())
            .build_cartesian_2d(x_range.clone(), y_range)?;
        chart.configure_mesh().bold_line_style(&WHITE).draw()?;
        for (j, line_spec) in line_specs.1.iter().enumerate() {
            chart.draw_series(LineSeries::new(
                skipped_data.clone().map(|x| (x.time, line_spec(x))),
                color_wheel[j],
            ))?;
        }
    }

    Ok(out_file.clone())
}

fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

static EXCHANGE: &str = "Binance";
static CHANNEL: &str = "order_books";

#[tokio::test]
async fn model_backtest() {
    init();
    let path = util::test::test_dir();
    let db = get_or_create(path.as_ref(), vec![]);
    let mut dt = NaiveTradingStrategy::make_lm_table("BTC_USDT", "ETH_USDT", db, 500);
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
    records[0].iter().zip(records[1].iter()).take(500).for_each(|(l, r)| {
        dt.push(&DataRow {
            time: l.event_ms,
            left: l.into(),
            right: r.into(),
        })
    });

    let beta = covar_model::beta(dt.window());
    assert!(beta > 0.0, "beta {:?} should be positive", beta);
}

#[actix::test]
async fn complete_backtest() {
    init();
    let path = util::test::test_dir();
    let order_manager_addr = mock_manager(&path);
    let test_results_dir = test_results_dir(module_path!());
    let mut strat = NaiveTradingStrategy::new(
        &path,
        0.001,
        &Options {
            left: LEFT_PAIR.into(),
            right: RIGHT_PAIR.into(),
            beta_eval_freq: 1000,
            beta_sample_freq: "1min".to_string(),
            window_size: 2000,
            exchange: Exchange::Binance,
            threshold_long: -0.03,
            threshold_short: 0.03,
            stop_loss: -0.1,
            stop_gain: 0.075,
            initial_cap: 100.0,
            dry_mode: Some(true),
            order_mode: OrderMode::Limit,
        },
        order_manager_addr,
    );
    // Read downsampled streams
    let records = input::load_csv_records(
        Utc.ymd(2021, 8, 2),
        Utc.ymd(2021, 8, 6),
        vec![LEFT_PAIR, RIGHT_PAIR],
        EXCHANGE,
        CHANNEL,
    )
    .await;

    let mut elapsed = 0_u128;
    let left_records = records[0].clone();
    let right_records = records[1].clone();
    assert!(!left_records.is_empty(), "no left pair records in dataset");
    assert!(!right_records.is_empty(), "no right pair records in dataset");
    let (zip, other) = left_records.iter().zip(right_records.iter()).tee();
    let (left, right) = other.tee();
    assert_eq!(left.len(), right.len(), "data should be aligned");
    let mut logs: Vec<StrategyLog> = Vec::new();
    let mut model_values: Vec<(DateTime<Utc>, LinearModelValue, f64, f64, f64)> = Vec::new();
    let mut trade_events: Vec<(OperationEvent, TradeEvent)> = Vec::new();
    let before_evals = Instant::now();
    let num_records = zip.len();

    for (l, r) in zip {
        let now = Instant::now();
        let row_time = l.event_ms;
        let row = DataRow {
            time: row_time,
            left: l.into(),
            right: r.into(),
        };
        strat.process_row(&row).await;
        if let Some(value) = strat.model_value() {
            model_values.push((
                row_time,
                value.clone(),
                strat.state.predicted_right(),
                strat.state.res(),
                strat.state.value_strat(),
            ));
        }
        logs.push(StrategyLog::from_state(
            row_time,
            &strat.state,
            &row,
            strat.model_value(),
        ));
        if let Some(op) = strat.state.ongoing_op() {
            for trade_event in op.trade_events() {
                trade_events.push((op.operation_event(), trade_event))
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

    write_model_values(&test_results_dir, &model_values);
    crate::test_util::log::write_trade_events(&test_results_dir, &trade_events);

    // Find that latest operations are correct
    let mut positions = strat.get_operations();
    positions.sort_by(|p1, p2| p1.pos.time.cmp(&p2.pos.time));
    let last_position = positions.last();
    assert_eq!(Some(162.130004882813), last_position.map(|p| p.pos.left_price));
    assert_eq!(Some(33.33032942489664), last_position.map(|p| p.left_value()));

    let out_file = draw_line_plot(logs).expect("Should have drawn plots from strategy logs");
    let copied = std::fs::copy(&out_file, "graphs/mean_reverting_plot_latest.svg");
    assert!(copied.is_ok(), "{}", format!("{:?} : {}", copied, out_file));
}

fn write_model_values(test_results_dir: &str, model_values: &[(DateTime<Utc>, LinearModelValue, f64, f64, f64)]) {
    crate::test_util::log::write_csv(
        format!("{}/model_values.csv", test_results_dir),
        &["ts", "beta", "alpha", "predicted_right", "res", "value_strat"],
        model_values.iter().map(|r| {
            vec![
                r.0.format(util::date::TIMESTAMP_FORMAT).to_string(),
                r.1.beta.to_string(),
                r.1.alpha.to_string(),
                r.2.to_string(),
                r.3.to_string(),
                r.4.to_string(),
            ]
        }),
    )
}
