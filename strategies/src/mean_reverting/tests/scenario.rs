use async_std::task;
use chrono::{Date, DateTime, TimeZone, Utc};
use coinnect_rt::exchange::Exchange;
use ordered_float::OrderedFloat;
use plotters::prelude::*;
use std::error::Error;
use std::time::{Duration, Instant};
use util::date::{DateRange, DurationRangeType};

use crate::input;
use crate::mean_reverting::ema_model::{MeanRevertingModelValue, SinglePosRow};
use crate::mean_reverting::options::Options;
use crate::mean_reverting::state::MeanRevertingState;
use crate::mean_reverting::MeanRevertingStrategy;
use crate::order_manager::test_util::mock_manager;
//use crate::test_util::tracing::setup_opentelemetry;
use crate::input::CsvRecord;
use crate::test_util::init;
use crate::types::{OperationEvent, OrderMode, TradeEvent};
use db::get_or_create;
use std::sync::Arc;
use tracing_futures::Instrument;
use util::date::now_str;

#[derive(Debug, Serialize, Clone)]
struct StrategyLog {
    time: DateTime<Utc>,
    mid: f64,
    threshold_short: f64,
    threshold_long: f64,
    apo: f64,
    pnl: f64,
    long_position_return: f64,
    short_position_return: f64,
    nominal_position: f64,
    value: MeanRevertingModelValue,
}

impl StrategyLog {
    fn from_state(
        time: DateTime<Utc>,
        state: &MeanRevertingState,
        last_row: &SinglePosRow,
        value: MeanRevertingModelValue,
    ) -> StrategyLog {
        StrategyLog {
            time,
            mid: last_row.pos.mid,
            threshold_short: state.threshold_short(),
            threshold_long: state.threshold_long(),
            apo: state.apo(),
            pnl: state.pnl(),
            long_position_return: state.long_position_return(),
            value,
            short_position_return: state.short_position_return(),
            nominal_position: state.nominal_position(),
        }
    }
}

fn draw_line_plot(data: Vec<StrategyLog>) -> std::result::Result<String, Box<dyn Error>> {
    std::fs::create_dir_all("graphs").unwrap();
    let out_file = format!("graphs/mean_reverting_plot_{}.svg", now_str());
    let color_wheel = vec![&BLACK, &BLUE, &RED];
    let more_lines: Vec<StrategyEntry<'_>> = vec![
        ("Prices and EMA", vec![|x| x.mid, |x| x.value.short_ema.current, |x| {
            x.value.long_ema.current
        }]),
        ("Open Position Return", vec![|x| x.short_position_return, |x| {
            x.long_position_return
        }]),
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

async fn load_csv_records(from: Date<Utc>, to: Date<Utc>) -> Vec<Vec<CsvRecord>> {
    let now = Instant::now();
    let csv_records = input::load_csv_dataset(
        &DateRange(from, to, DurationRangeType::Days, 1),
        vec![PAIR.to_string()],
        EXCHANGE,
        CHANNEL,
    )
    .await;
    let num_records = csv_records[0].len();
    assert!(num_records > 0, "no csv records could be read");
    info!(
        "Loaded {} csv records in {:.6} ms",
        num_records,
        now.elapsed().as_millis()
    );
    csv_records
}

#[tokio::test]
async fn moving_average_model_backtest() {
    init();
    let path = util::test::test_dir();
    let db = get_or_create(path.as_ref(), vec![]);
    let mut model = MeanRevertingStrategy::make_model("BTC_USDT", Arc::new(db), 100, 1000);
    let csv_records = load_csv_records(Utc.ymd(2020, 3, 27), Utc.ymd(2020, 4, 8)).await;
    csv_records[0].iter().take(500).for_each(|l| {
        model
            .update_model(SinglePosRow {
                time: l.event_ms,
                pos: l.into(),
            })
            .unwrap();
    });
    let apo = model.value().unwrap().apo;
    assert!(apo != 0.0, "apo should not be 0, was: {}", apo);
}

#[actix::test]
async fn complete_backtest() {
    init();
    //setup_opentelemetry();
    let _window_size = 10000;
    let path = util::test::test_dir();
    let order_manager_addr = mock_manager(&path);
    task::sleep(Duration::from_millis(20)).await;
    let module_path = module_path!().replace("::", "_");
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let test_results_dir = &format!("{}/test_results/{}", manifest_dir, module_path);
    std::fs::create_dir_all(test_results_dir).unwrap();

    let mut strat = MeanRevertingStrategy::new(
        path,
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
            order_mode: OrderMode::Limit,
        },
        order_manager_addr,
    );
    let mut elapsed = 0_u128;
    let csv_records = load_csv_records(Utc.ymd(2020, 3, 27), Utc.ymd(2020, 4, 8)).await;
    let num_records = csv_records.len();
    // align data
    let pair_csv_records = csv_records[0].iter();
    let mut strategy_logs: Vec<StrategyLog> = Vec::new();
    let mut model_values: Vec<(DateTime<Utc>, MeanRevertingModelValue, f64)> = Vec::new();
    let mut trade_events: Vec<(OperationEvent, TradeEvent)> = Vec::new();

    // Feed all csv records to the strat
    let before_evals = Instant::now();
    for csvr in pair_csv_records {
        let now = Instant::now();
        let row_time = csvr.event_ms;
        let row = SinglePosRow {
            time: row_time,
            pos: csvr.into(),
        };

        strat
            .process_row(&row)
            .instrument(tracing::debug_span!("process_row"))
            .await;

        let value = strat.model_value().unwrap();
        model_values.push((row_time, value.clone(), strat.state.value_strat()));
        strategy_logs.push(StrategyLog::from_state(row_time, &strat.state, &row, value));
        match strat.state.ongoing_op() {
            Some(op) => trade_events.push((op.operation_event().clone(), op.trade_event())),
            None => (),
        }
        elapsed += now.elapsed().as_nanos();
    }
    info!(
        "For {} records, evals took {}ms, each iteration took {} ns on avg",
        num_records,
        before_evals.elapsed().as_millis(),
        elapsed / num_records as u128
    );

    // Write all model values to a csv file
    write_ema_values(test_results_dir, &model_values);

    // Write all trade events to a csv file
    write_trade_events(test_results_dir, &trade_events);

    // Write strategy logs
    write_thresholds(test_results_dir, &mut strategy_logs);

    // Find that latest operations are correct
    let mut positions = strat.get_operations();
    positions.sort_by(|p1, p2| p1.pos.time.cmp(&p2.pos.time));
    let last_position = positions.last();
    assert!(last_position.is_some(), "No position found in operations");
    // Output SVG graphs
    let out_file = draw_line_plot(strategy_logs).expect("Should have drawn plots from strategy logs");
    let copied = std::fs::copy(&out_file, "graphs/mean_reverting_plot_latest.svg");
    assert!(copied.is_ok(), "{}", format!("{:?} : {}", copied, out_file));

    assert_eq!(Some(78767.08484500754), last_position.map(|p| p.pos.price));
    assert_eq!(Some(90.98012915244145), last_position.map(|p| p.value()));
}

fn write_trade_events(test_results_dir: &str, trade_events: &[(OperationEvent, TradeEvent)]) {
    let mut trade_events_wtr =
        csv::Writer::from_path(format!("{}/{}_trade_events.csv", test_results_dir, PAIR)).unwrap();
    trade_events_wtr
        .write_record(&["ts", "op", "pos", "trade_kind", "price", "qty", "value_strat"])
        .unwrap();
    for (op_event, trade_event) in trade_events {
        trade_events_wtr
            .write_record(&[
                trade_event.at.format(util::date::TIMESTAMP_FORMAT).to_string(),
                op_event.op.as_ref().to_string(),
                op_event.pos.as_ref().to_string(),
                trade_event.op.as_ref().to_string(),
                trade_event.price.to_string(),
                trade_event.qty.to_string(),
                trade_event.strat_value.to_string(),
            ])
            .unwrap();
    }

    trade_events_wtr.flush().unwrap();
}

fn write_ema_values(test_results_dir: &str, model_values: &[(DateTime<Utc>, MeanRevertingModelValue, f64)]) {
    let mut ema_values_wtr = csv::Writer::from_path(format!("{}/{}_ema_values.csv", test_results_dir, PAIR)).unwrap();
    ema_values_wtr
        .write_record(&["ts", "short_ema", "long_ema", "apo", "value_strat"])
        .unwrap();
    for model_value in model_values {
        ema_values_wtr
            .write_record(&[
                model_value.0.format(util::date::TIMESTAMP_FORMAT).to_string(),
                model_value.1.short_ema.current.to_string(),
                model_value.1.long_ema.current.to_string(),
                model_value.1.apo.to_string(),
                model_value.2.to_string(),
            ])
            .unwrap();
    }
    ema_values_wtr.flush().unwrap();
}

fn write_thresholds(test_results_dir: &str, strategy_logs: &mut Vec<StrategyLog>) {
    {
        info_time!("Write strategy logs");
        let mut thresholds_wtr =
            csv::Writer::from_path(format!("{}/{}_thresholds.csv", test_results_dir, PAIR)).unwrap();
        thresholds_wtr
            .write_record(&["ts", "threshold_short", "threshold_long"])
            .unwrap();
        //let logs_f = std::fs::File::create("strategy_logs.json").unwrap();
        for log in strategy_logs.clone() {
            thresholds_wtr
                .write_record(&[
                    log.time.format(util::date::TIMESTAMP_FORMAT).to_string(),
                    log.threshold_short.to_string(),
                    log.threshold_long.to_string(),
                ])
                .unwrap();
        }
        thresholds_wtr.flush().unwrap();
    }
}
