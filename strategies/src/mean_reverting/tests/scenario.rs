use std::error::Error;
use std::time::Instant;

use chrono::{DateTime, TimeZone, Utc};
use ordered_float::OrderedFloat;
use plotters::prelude::*;
use tokio::time::Duration;

use coinnect_rt::prelude::*;
use db::DbOptions;
use math::indicators::macd_apo::MACDApo;
use util::date::now_str;
use util::test::test_results_dir;

use crate::driver::StrategyDriver;
use crate::input;
use crate::margin_interest_rates::test_util::mock_interest_rate_provider;
use crate::mean_reverting::ema_model::ema_indicator_model;
use crate::mean_reverting::options::Options;
use crate::mean_reverting::state::MeanRevertingState;
use crate::mean_reverting::MeanRevertingStrategy;
use crate::order_manager::test_util::mock_manager;
use crate::test_util::{init, test_db};
use crate::types::{BookPosition, OperationEvent, TradeEvent};

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
    fn from_state(time: DateTime<Utc>, state: &MeanRevertingState, ob: &Orderbook, value: MACDApo) -> StrategyLog {
        StrategyLog {
            time,
            mid: ob.avg_price().unwrap(),
            threshold_short: state.threshold_short(),
            threshold_long: state.threshold_long(),
            apo: value.apo,
            pnl: state.pnl(),
            position_return: state.position_return(),
            value,
            nominal_position: state.nominal_position(),
        }
    }
}

fn draw_line_plot(data: Vec<StrategyLog>) -> std::result::Result<String, Box<dyn Error>> {
    let graph_dir = format!("{}/graphs", util::test::test_results_dir(module_path!()),);
    std::fs::create_dir_all(&graph_dir).unwrap();
    let out_file = format!("{}/mean_reverting_plot_{}.svg", graph_dir, now_str());
    let color_wheel = vec![&BLACK, &BLUE, &RED];
    let more_lines: Vec<StrategyEntry<'_>> = vec![
        ("Prices and EMA", vec![|x| x.mid, |x| x.value.short_ema.current, |x| {
            x.value.long_ema.current
        }]),
        ("Open Position Return", vec![|x| x.position_return]),
        ("APO", vec![|x| x.apo]),
        ("PnL", vec![|x| x.pnl]),
        ("Nominal (units)", vec![|x| x.nominal_position]),
    ];
    let height: u32 = 342 * more_lines.len() as u32;
    let root = SVGBackend::new(&out_file, (1724, height)).into_drawing_area();
    root.fill(&WHITE)?;

    let lower = data.first().unwrap().time;
    let upper = data.last().unwrap().time;
    let x_range = lower..upper;

    let area_rows = root.split_evenly((more_lines.len(), 1));

    let skipped_data = data.iter();
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

type StrategyEntry<'a> = (&'a str, Vec<fn(&StrategyLog) -> f64>);

static EXCHANGE: &str = "Binance";
static CHANNEL: &str = "order_books";
static PAIR: &str = "BTC_USDT";

#[tokio::test]
async fn moving_average_model_backtest() {
    init();
    let db = test_db();
    let mut model = ema_indicator_model(PAIR, db, 100, 1000);
    let csv_records =
        input::load_csv_records(Utc.ymd(2020, 8, 1), Utc.ymd(2020, 8, 9), vec![PAIR], EXCHANGE, CHANNEL).await;
    csv_records[0].iter().take(500).for_each(|l| {
        let pos: BookPosition = l.into();
        model.update(pos.mid).unwrap();
    });
    let apo = model.value().unwrap().apo;
    assert!(apo != 0.0, "apo should not be 0, was: {}", apo);
}

#[actix::test]
async fn complete_backtest() {
    init();
    //setup_opentelemetry();
    let path = util::test::test_dir();
    let order_manager_addr = mock_manager(&path);
    let margin_interest_rate_provider_addr = mock_interest_rate_provider(Exchange::Binance);
    let test_results_dir = test_results_dir(module_path!());

    let mut strat = MeanRevertingStrategy::new(
        &DbOptions::new(path),
        0.001,
        &Options {
            pair: PAIR.into(),
            threshold_long: -0.01,
            threshold_short: 0.01,
            threshold_eval_freq: Some(1),
            dynamic_threshold: Some(true),
            threshold_window_size: Some(10000),
            stop_loss: -0.1,
            stop_gain: 0.075,
            initial_cap: 100.0,
            dry_mode: Some(true),
            short_window_size: 100,
            long_window_size: 1000,
            sample_freq: "1min".to_string(),
            exchange: Exchange::Binance,
            order_mode: None,
            execution_instruction: None,
            order_asset_type: None,
        },
        order_manager_addr,
        margin_interest_rate_provider_addr,
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
    for row in pair_csv_records.map(|csvr| LiveEventEnvelope {
        xch: Exchange::from(EXCHANGE.to_string()),
        e: LiveEvent::LiveOrderbook(csvr.to_orderbook(PAIR)),
    }) {
        let now = Instant::now();

        strat.add_event(&row).await.unwrap();
        let mut tries = 0;
        loop {
            if tries > 5 {
                break;
            }
            if let Some(op) = strat.state.ongoing_op().cloned() {
                strat.resolve_orders().await;
                if strat.state.ongoing_op().is_none() {
                    trade_events.push((op.operation_event().clone(), op.trade_event()))
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                tries += 1;
            } else {
                break;
            }
        }
        let value = strat.model_value().unwrap();
        let row_time = row.e.time();
        model_values.push((row_time, value.clone(), strat.state.value_strat()));
        if let LiveEvent::LiveOrderbook(ob) = row.e {
            strategy_logs.push(StrategyLog::from_state(row_time, &strat.state, &ob, value));
        }
        elapsed += now.elapsed().as_nanos();
    }
    info!(
        "For {} records, evals took {}ms, each iteration took {} ns on avg",
        num_records,
        before_evals.elapsed().as_millis(),
        elapsed / num_records as u128
    );

    write_ema_values(&test_results_dir, &model_values);
    crate::test_util::log::write_trade_events(&test_results_dir, &trade_events);
    write_thresholds(&test_results_dir, &strategy_logs);

    // Find that latest operations are correct
    let mut positions = strat.get_operations();
    positions.sort_by(|p1, p2| p1.pos.time.cmp(&p2.pos.time));
    let last_position = positions.last();
    assert!(last_position.is_some(), "No position found in operations");
    // Output SVG graphs
    let _out_file = draw_line_plot(strategy_logs).expect("Should have drawn plots from strategy logs");
    // let copied = std::fs::copy(&out_file, ".local_data/graphs/mean_reverting_plot_latest.svg");
    // assert!(copied.is_ok(), "{}", format!("{:?} : {}", copied, out_file));

    assert_eq!(
        Some("44015.99".to_string()),
        last_position.map(|p| format!("{:.2}", p.pos.price))
    );
    assert_eq!(
        Some("88.42".to_string()),
        last_position.map(|p| format!("{:.2}", p.value()))
    );
}

fn write_ema_values(test_results_dir: &str, model_values: &[(DateTime<Utc>, MACDApo, f64)]) {
    crate::test_util::log::write_csv(
        format!("{}/{}_ema_values.csv", test_results_dir, PAIR),
        &["ts", "short_ema", "long_ema", "apo", "value_strat"],
        model_values.iter().map(|r| {
            vec![
                r.0.format(util::date::TIMESTAMP_FORMAT).to_string(),
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
                l.time.format(util::date::TIMESTAMP_FORMAT).to_string(),
                l.threshold_short.to_string(),
                l.threshold_long.to_string(),
            ]
        }),
    );
}
