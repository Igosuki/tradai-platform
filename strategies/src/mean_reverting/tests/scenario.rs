use async_std::task;
use chrono::{DateTime, TimeZone, Utc};
use plotters::prelude::*;
use std::error::Error;
use std::time::{Duration, Instant};
use util::date::{DateRange, DurationRangeType};

use crate::input::{self, to_pos};
use crate::mean_reverting::ema_model::{MeanRevertingModelValue, SinglePosRow};
use crate::mean_reverting::options::Options;
use crate::mean_reverting::state::MeanRevertingState;
use crate::mean_reverting::MeanRevertingStrategy;
use crate::order_manager::test_util;
use ordered_float::OrderedFloat;

#[derive(Debug, Serialize)]
struct StrategyLog {
    time: DateTime<Utc>,
    mid: f64,
    state: MeanRevertingState,
    value: MeanRevertingModelValue,
}

impl StrategyLog {
    fn from_state(
        time: DateTime<Utc>,
        state: MeanRevertingState,
        last_row: &SinglePosRow,
        value: MeanRevertingModelValue,
    ) -> StrategyLog {
        StrategyLog {
            time,
            mid: last_row.pos.mid,
            state,
            value,
        }
    }
}

fn now_str() -> String {
    let now = Utc::now();
    now.format("%Y%m%d%H:%M:%S").to_string()
}

fn draw_line_plot(data: Vec<StrategyLog>) -> std::result::Result<String, Box<dyn Error>> {
    let string = format!("graphs/mean_reverting_plot_{}.svg", now_str());
    let color_wheel = vec![&BLACK, &BLUE, &RED];
    let more_lines: Vec<StrategyEntry<'_>> = vec![
        (
            "Prices and EMA",
            vec![|x| x.mid, |x| x.value.short_ema.current, |x| {
                x.value.long_ema.current
            }],
        ),
        (
            "Open Position Return",
            vec![|x| x.state.short_position_return(), |x| {
                x.state.long_position_return()
            }],
        ),
        ("APO", vec![|x| x.state.apo()]),
        ("PnL", vec![|x| x.state.pnl()]),
        ("Nominal (units)", vec![|x| x.state.nominal_position()]),
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

type StrategyEntry<'a> = (&'a str, Vec<fn(&StrategyLog) -> f64>);

fn init() {
    let _ = env_logger::builder().is_test(true).try_init();
}

static EXCHANGE: &str = "Binance";
static CHANNEL: &str = "order_books";
static PAIR: &str = "BTC_USDT";

#[tokio::test]
async fn moving_average() {
    init();
    let mut dt = MeanRevertingStrategy::make_model_table("BTC_USDT", "default", 100, 1000);
    // Read downsampled streams
    let dt0 = Utc.ymd(2020, 3, 25);
    let dt1 = Utc.ymd(2020, 3, 25);
    let records = input::load_csv_dataset(
        &DateRange(dt0, dt1, DurationRangeType::Days, 1),
        vec![PAIR.to_string()],
        EXCHANGE,
        CHANNEL,
    )
        .await;
    // align data
    records[0]
        .iter()
        .zip(records[1].iter())
        .take(500)
        .for_each(|(l, r)| {
            dt.update_model(&SinglePosRow {
                time: l.event_ms,
                pos: to_pos(r),
            })
                .unwrap();
        });
    let model_value = dt.model().unwrap().value;
    let apo = model_value.apo;
    println!("apo {}", apo);
    assert!(apo > 0.0, "{}", apo);
}

#[actix_rt::test]
async fn continuous_scenario() {
    init();
    let _window_size = 10000;
    let path = crate::test_util::test_dir();
    let order_manager_addr = test_util::mock_manager(&path);
    task::sleep(Duration::from_secs(1)).await;
    let test_results_dir = "test_results";
    std::fs::create_dir_all(test_results_dir).unwrap();
    let mut strat = MeanRevertingStrategy::new(
        &path,
        0.001,
        &Options {
            pair: PAIR.into(),
            threshold_long: -0.01,
            threshold_short: 0.01,
            threshold_eval_freq: None,
            dynamic_threshold: None,
            threshold_window_size: None,
            stop_loss: -0.1,
            stop_gain: 0.075,
            initial_cap: 100.0,
            dry_mode: Some(true),
            short_window_size: 100,
            long_window_size: 1000,
            sample_freq: "1min".to_string(),
        },
        order_manager_addr,
    );
    // Read downsampled streams
    let dt0 = Utc.ymd(2020, 3, 25);
    let dt1 = Utc.ymd(2020, 4, 8);
    // align data
    let mut elapsed = 0 as u128;
    let mut iterations = 1 as u128;
    let records = input::load_csv_dataset(
        &DateRange(dt0, dt1, DurationRangeType::Days, 1),
        vec![PAIR.to_string()],
        EXCHANGE,
        CHANNEL,
    )
        .await;
    println!("{}", records[0].len());
    println!("Dataset loaded in memory...");
    // align data
    let eval = records[0].iter();
    let mut logs: Vec<StrategyLog> = Vec::new();
    let mut model_values: Vec<(DateTime<Utc>, MeanRevertingModelValue)> = Vec::new();
    let mut wtr =
        csv::Writer::from_path(format!("{}/ema_values_{}.csv", test_results_dir, now_str()))
            .unwrap();

    // Feed all csv records to the strat
    for csvr in eval {
        iterations += 1;
        if iterations % 1000 == 0 {
            info!("Reached {} iterations", iterations);
        }
        let now = Instant::now();

        let log = {
            let row_time = csvr.event_ms;
            let row = SinglePosRow {
                time: row_time,
                pos: to_pos(csvr),
            };
            strat.process_row(&row).await;
            let value = strat.model_value().unwrap();
            model_values.push((row_time, value.clone()));
            StrategyLog::from_state(row_time, strat.state.clone(), &row, value)
        };
        elapsed += now.elapsed().as_nanos();
        logs.push(log);
    }
    println!("Each iteration took {} on avg", elapsed / iterations);

    // Write all model values to a csv file
    for model_value in model_values {
        wtr.write_record(&[
            model_value.0.format("%Y%m%d%H:%M:%S").to_string(),
            model_value.1.short_ema.current.to_string(),
            model_value.1.long_ema.current.to_string(),
            model_value.1.apo.to_string(),
        ])
            .unwrap();
    }
    wtr.flush().unwrap();

    // Find that latest operations are correct
    let mut positions = strat.get_operations();
    positions.sort_by(|p1, p2| p1.pos.time.cmp(&p2.pos.time));
    let last_position = positions.last();

    let logs_f = std::fs::File::create("strategy_logs.json").unwrap();
    serde_json::to_writer(logs_f, &logs).unwrap();
    std::fs::create_dir_all("graphs").unwrap();
    let drew = draw_line_plot(logs);
    if let Ok(file) = drew {
        let copied = std::fs::copy(&file, "graphs/mean_reverting_plot_latest.svg");
        assert!(copied.is_ok(), "{}", format!("{:?}", copied));
    } else {
        panic!("{}", format!("{:?}", drew));
    }

    assert_eq!(Some(162.130004882813), last_position.map(|p| p.pos.price));
    assert_eq!(Some(33.33032942489664), last_position.map(|p| p.value()));

    drop(strat);
}
