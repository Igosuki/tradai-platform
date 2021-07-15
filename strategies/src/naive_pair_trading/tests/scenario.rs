use async_std::task;
use chrono::{DateTime, TimeZone, Utc};
use plotters::prelude::*;
use serde::Serialize;

use crate::naive_pair_trading::options::Options;
use crate::naive_pair_trading::state::MovingState;
use crate::naive_pair_trading::{covar_model, DataRow, NaiveTradingStrategy};
use crate::order_manager::test_util;
use coinnect_rt::exchange::Exchange;
use db::get_or_create;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};
use util::date::{DateRange, DurationRangeType};

static LEFT_PAIR: &str = "ETH_USDT";
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
    fn from_state(time: DateTime<Utc>, state: &MovingState, last_row: &DataRow) -> StrategyLog {
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
    let now = Utc::now();
    let string = format!("graphs/naive_pair_trading_plot_{}.svg", now.format("%Y%m%d%H:%M:%S"));
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
    let root = SVGBackend::new(&string, (1724, height)).into_drawing_area();
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

    Ok(string.clone())
}

fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

static EXCHANGE: &str = "Binance";
static CHANNEL: &str = "order_books";

#[tokio::test]
async fn model_backtest() {
    init();
    let path = util::test::test_dir();
    let db = Arc::new(get_or_create(path.as_ref(), vec![]));
    let mut dt = NaiveTradingStrategy::make_lm_table("BTC_USDT", "ETH_USDT", db, 500);
    // Read downsampled streams
    let dt0 = Utc.ymd(2020, 3, 25);
    let dt1 = Utc.ymd(2020, 3, 25);
    let records = crate::input::load_csv_dataset(
        &DateRange(dt0, dt1, DurationRangeType::Days, 1),
        vec![LEFT_PAIR.to_string(), RIGHT_PAIR.to_string()],
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

#[actix_rt::test]
async fn complete_backtest() {
    init();
    let path = util::test::test_dir();
    let beta_eval_freq = 1000;
    let window_size = 2000;
    let order_manager_addr = test_util::mock_manager(&path);
    task::sleep(Duration::from_millis(20)).await;
    let mut strat = NaiveTradingStrategy::new(
        &path,
        0.001,
        &Options {
            left: LEFT_PAIR.into(),
            right: RIGHT_PAIR.into(),
            beta_eval_freq,
            beta_sample_freq: "1min".to_string(),
            window_size,
            exchange: Exchange::Binance,
            threshold_long: -0.03,
            threshold_short: 0.03,
            stop_loss: -0.1,
            stop_gain: 0.075,
            initial_cap: 100.0,
            dry_mode: Some(true),
        },
        order_manager_addr,
    );
    // Read downsampled streams
    let dt0 = Utc.ymd(2020, 3, 25);
    let dt1 = Utc.ymd(2020, 4, 8);
    let records = crate::input::load_csv_dataset(
        &DateRange(dt0, dt1, DurationRangeType::Days, 1),
        vec![LEFT_PAIR.to_string(), RIGHT_PAIR.to_string()],
        EXCHANGE,
        CHANNEL,
    )
    .await;
    println!("Dataset loaded in memory...");
    // align data
    let mut elapsed = 0_u128;
    let mut iterations = 0_u128;
    let left_records = records[0].clone();
    let right_records = records[1].clone();
    assert!(!left_records.is_empty(), "no left pair records in dataset");
    assert!(!right_records.is_empty(), "no right pair records in dataset");
    let (zip, other) = left_records.iter().zip(right_records.iter()).tee();
    let (_left, _right) = other.tee();
    let mut logs: Vec<StrategyLog> = Vec::new();
    for (l, r) in zip {
        iterations += 1;
        let now = Instant::now();

        if iterations as i32 % (beta_eval_freq + window_size - 1) == 0
            || (iterations as i32 % window_size == 0 && !strat.data_table.has_model())
        {
            // simulate a model update ever n because we cannot simulate time
            // strat.eval_linear_model();
            // debug!("{:?}", strat.data_table.last_model_time());
        }
        let log = {
            let row_time = l.event_ms;
            let row = DataRow {
                time: row_time,
                left: l.into(),
                right: r.into(),
            };
            strat.process_row(&row).await;
            StrategyLog::from_state(row_time, &strat.state, &row)
        };
        elapsed += now.elapsed().as_nanos();
        logs.push(log);
    }
    println!("Each iteration took {} on avg", elapsed / iterations);

    let mut positions = strat.get_operations();
    positions.sort_by(|p1, p2| p1.pos.time.cmp(&p2.pos.time));
    let last_position = positions.last();
    assert_eq!(Some(162.130004882813), last_position.map(|p| p.pos.left_price));
    assert_eq!(Some(33.33032942489664), last_position.map(|p| p.left_value()));

    // let logs_f = std::fs::File::create("strategy_logs.json").unwrap();
    // serde_json::to_writer(logs_f, &logs);
    std::fs::create_dir_all("graphs").unwrap();
    let drew = draw_line_plot(logs);
    if let Ok(file) = drew {
        let copied = std::fs::copy(&file, "graphs/naive_pair_trading_plot_latest.svg");
        assert!(copied.is_ok(), "{}", format!("{:?}", copied));
    } else {
        panic!("{}", format!("{:?}", drew));
    }
}
