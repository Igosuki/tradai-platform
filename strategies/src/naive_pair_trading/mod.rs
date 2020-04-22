use chrono::{DateTime, Duration, Utc};
use coinnect_rt::types::{BigDecimalConv, LiveEvent, Orderbook};
use itertools::Itertools;
use std::sync::Arc;

pub mod input;
pub mod metrics;
pub mod state;

use std::ops::Add;
use thiserror::Error;

use crate::StrategySink;
use db::Db;
use math::iter::{CovarianceExt, MeanExt, VarianceExt};
use metrics::StrategyMetrics;
use state::{MovingState, Position, PositionKind};
use uuid::Uuid;

pub struct Strategy {
    fees_rate: f64,
    res_threshold_long: f64,
    res_threshold_short: f64,
    stop_loss: f64,
    evaluations_since_last: i32,
    beta_eval_window_size: i32,
    beta_eval_freq: i32,
    #[allow(dead_code)]
    beta_sample_freq: Duration,
    state: MovingState,
    data_table: DataTable,
    pub right_pair: String,
    pub left_pair: String,
    #[allow(dead_code)]
    metrics: Arc<StrategyMetrics>,
    db: Db,
    last_row_process_time: DateTime<Utc>,
    last_left: Option<BookPosition>,
    last_right: Option<BookPosition>,
}

impl Strategy {
    pub fn new(
        left_pair: &str,
        right_pair: &str,
        beta_eval_freq: i32,
        beta_sample_freq: Duration,
    ) -> Self {
        let db_name = format!("{}_{}", left_pair, right_pair);
        let metrics =
            StrategyMetrics::for_strat(prometheus::default_registry(), left_pair, right_pair);
        Self {
            fees_rate: 0.001,
            res_threshold_long: -0.04,
            res_threshold_short: 0.04,
            stop_loss: -0.1,
            evaluations_since_last: 0,
            beta_eval_window_size: 500,
            beta_eval_freq,
            state: MovingState::new(100.0),
            data_table: DataTable::new(500),
            right_pair: right_pair.to_string(),
            left_pair: left_pair.to_string(),
            last_row_process_time: Utc::now(),
            metrics: Arc::new(metrics),
            db: Db::new("data/naive_pair_trading", db_name),
            last_left: None,
            last_right: None,
            beta_sample_freq,
        }
    }

    fn inc_evaluations_since_last(&mut self) {
        self.evaluations_since_last += 1;
    }

    fn log_stop_loss(&self, pos: PositionKind) {
        debug!(
            "---- Stop-loss executed ({} position) ----",
            match pos {
                PositionKind::SHORT => "short",
                PositionKind::LONG => "long",
            }
        )
    }

    fn should_eval(&self) -> bool {
        let x = (self.evaluations_since_last % self.beta_eval_freq) == 0;
        if x {
            trace!(
                "eval_time : {} % {} == 0",
                self.evaluations_since_last,
                self.beta_eval_freq
            );
        }
        x
    }

    fn update_model(&mut self) {
        let beta = self.data_table.beta();
        self.state.set_beta(beta);
        self.state.set_alpha(self.data_table.alpha(beta));
    }

    fn predict(&self, bp: &BookPosition) -> f64 {
        self.data_table
            .predict(self.state.alpha(), self.state.beta(), bp)
    }

    fn set_long_spread(&mut self, traded_price: f64) {
        self.state.set_units_to_buy_long_spread(
            self.state.value_strat() / (traded_price * (1.0 + self.fees_rate)),
        );
    }

    fn set_short_spread(&mut self, traded_price: f64) {
        self.state.set_units_to_buy_short_spread(
            self.state.value_strat() / (traded_price * self.state.beta() * (1.0 + self.fees_rate)),
        );
    }

    fn short_position(&self, right_price: f64, left_price: f64, time: DateTime<Utc>) -> Position {
        Position {
            kind: PositionKind::SHORT,
            right_price,
            left_price,
            time,
            right_pair: self.right_pair.to_string(),
            left_pair: self.left_pair.to_string(),
        }
    }

    fn long_position(&self, right_price: f64, left_price: f64, time: DateTime<Utc>) -> Position {
        Position {
            kind: PositionKind::LONG,
            right_price,
            left_price,
            time,
            right_pair: self.right_pair.to_string(),
            left_pair: self.left_pair.to_string(),
        }
    }

    fn eval_latest(&mut self, lr: &DataRow) {
        if self.state.no_position_taken() {
            if self.should_eval() {
                self.update_model();
            }
            self.set_long_spread(lr.right.ask);
            self.set_short_spread(lr.left.ask);
        }

        self.state.set_beta_lr();
        self.state.set_predicted_right(self.predict(&lr.left));

        if self.state.beta_lr() >= 0.0 {
            self.state
                .set_res((lr.right.mid - self.state.predicted_right()) / lr.right.mid);
        } else {
            self.state.set_res(0.0);
        }
        if (self.state.res() > self.res_threshold_short) && self.state.no_position_taken() {
            let position = self.short_position(lr.right.bid, lr.left.ask, lr.time);
            self.log_position(&position);
            self.state.open(position, self.fees_rate);
        }

        if self.state.is_short() {
            let short_position_return =
                self.state
                    .set_short_position_return(self.fees_rate, lr.right.ask, lr.left.bid);
            if (self.state.res() <= self.res_threshold_short && self.state.res() < 0.0)
                || short_position_return < self.stop_loss
            {
                if short_position_return < self.stop_loss {
                    self.log_stop_loss(PositionKind::SHORT);
                }
                let position = self.short_position(lr.right.ask, lr.left.bid, lr.time);
                self.log_position(&position);
                self.state.close(position, self.fees_rate);
                self.state.set_pnl();
                self.update_model();
            }
        }

        if self.state.res() <= self.res_threshold_long && self.state.no_position_taken() {
            let position = self.long_position(lr.right.ask, lr.left.bid, lr.time);
            self.log_position(&position);
            self.state.open(position, self.fees_rate);
        }

        if self.state.is_long() {
            let long_position_return =
                self.state
                    .set_long_position_return(self.fees_rate, lr.right.bid, lr.left.ask);
            if (self.state.res() >= self.res_threshold_long && self.state.res() > 0.0)
                || long_position_return < self.stop_loss
            {
                if long_position_return < self.stop_loss {
                    self.log_stop_loss(PositionKind::LONG);
                }
                let position = self.long_position(lr.right.bid, lr.left.ask, lr.time);
                self.log_position(&position);
                self.state.close(position, self.fees_rate);
                self.state.set_pnl();
                self.update_model();
            }
        }
    }

    fn process_row(&mut self, row: &DataRow) {
        self.inc_evaluations_since_last();
        if self.data_table.rows.len() > self.beta_eval_window_size as usize {
            self.eval_latest(row);
            self.log_state();
        }
        self.metrics.log_row(&row);
        self.data_table.push(row);
        if self.data_table.rows.len() == self.beta_eval_window_size as usize {
            self.update_model();
            self.set_long_spread(row.right.ask);
            self.set_short_spread(row.left.ask);
            self.state.set_pnl();
        }
    }

    fn log_state(&self) {
        self.metrics.log_state(&self.state);
    }

    fn log_position(&self, pos: &Position) {
        self.db.put_json(&format!("orders:{}", Uuid::new_v4()), pos)
    }

    fn get_positions(&self) -> Vec<Position> {
        self.db.read_json_vec("orders")
    }

    #[allow(dead_code)]
    fn get_position(&self, uuid: &str) -> Option<Position> {
        self.db.read_json(&format!("orders:{}", uuid))
    }
}

impl StrategySink for Strategy {
    fn add_event(&mut self, le: LiveEvent) -> std::io::Result<()> {
        match le {
            LiveEvent::LiveOrderbook(ob) => {
                let string = ob.pair.as_string();
                if string == self.left_pair {
                    self.last_left = BookPosition::from_book(ob);
                } else if string == self.right_pair {
                    self.last_right = BookPosition::from_book(ob);
                }
            }
            _ => {}
        };
        let now = Utc::now();
        if now.gt(&self
            .last_row_process_time
            .add(chrono::Duration::milliseconds(200)))
        {
            match (self.last_left.clone(), self.last_right.clone()) {
                (Some(l), Some(r)) => {
                    let x = DataRow {
                        left: l,
                        right: r,
                        time: now,
                    };
                    self.last_row_process_time = now;
                    self.process_row(&x);
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct BookPosition {
    pub mid: f64,
    // mid = (top_ask + top_bid) / 2, alias: crypto1_m
    ask: f64,
    // crypto_a
    ask_q: f64,
    // crypto_a_q
    bid: f64,
    // crypto_b
    bid_q: f64, // crypto_b_q
}

impl BookPosition {
    fn new(ask: f64, ask_q: f64, bid: f64, bid_q: f64) -> Self {
        Self {
            ask,
            ask_q,
            bid,
            bid_q,
            mid: Self::mid(ask, bid),
        }
    }

    fn mid(ask: f64, bid: f64) -> f64 {
        (ask + bid) / 2.0
    }

    fn from_book(t: Orderbook) -> Option<BookPosition> {
        let first_ask = t
            .asks
            .first()
            .map(|a| (a.0.as_f64().unwrap(), a.1.as_f64().unwrap()));
        let first_bid = t
            .bids
            .first()
            .map(|a| (a.0.as_f64().unwrap(), a.1.as_f64().unwrap()));
        match (first_ask, first_bid) {
            (Some(ask), Some(bid)) => return Some(BookPosition::new(ask.0, ask.1, bid.0, bid.1)),
            _ => return None,
        }
    }
}

#[derive(Debug, Clone)]
struct DataRow {
    time: DateTime<Utc>,
    pub left: BookPosition,
    // crypto_1
    pub right: BookPosition, // crypto_2
}

#[derive(Debug)]
struct DataTable {
    rows: Vec<DataRow>,
    window_size: usize,
    max_size: usize,
}

impl DataTable {
    fn new(window_size: usize) -> Self {
        DataTable {
            rows: Vec::new(),
            window_size,
            max_size: window_size * 8, // Keep window_size * 8 elements
        }
    }

    #[allow(dead_code)]
    fn current_window(&self) -> &[DataRow] {
        let len = self.rows.len();
        if len <= self.window_size {
            &self.rows[..]
        } else {
            &self.rows[(len - self.window_size)..(len - 1)]
        }
    }

    pub fn push(&mut self, row: &DataRow) {
        self.rows.push(row.clone());
        // Truncate the table by window_size once max_size is reached
        if self.rows.len() > self.max_size {
            self.rows.drain(0..self.window_size);
        }
    }

    pub fn beta(&self) -> f64 {
        let (var, covar) = self.rows.iter().rev().take(self.window_size).tee();
        let variance: f64 = var.map(|r| r.left.mid).variance();
        trace!("variance {:?}", variance);
        let covariance: f64 = covar
            .map(|r| (r.left.mid, r.right.mid))
            .covariance::<(f64, f64), f64>();
        trace!("covariance {:?}", covariance);
        let beta_val = covariance / variance;
        trace!("beta_val {:?}", beta_val);
        beta_val
    }

    pub fn alpha(&self, beta_val: f64) -> f64 {
        let (left, right) = self.rows.iter().rev().take(self.window_size).tee();
        let mean_left: f64 = left.map(|l| l.left.mid).mean();
        trace!("mean left {:?}", mean_left);
        let mean_right: f64 = right.map(|l| l.right.mid).mean();
        trace!("mean right {:?}", mean_right);
        mean_right - beta_val * mean_left
    }

    fn predict(&self, alpha_val: f64, beta_val: f64, bp: &BookPosition) -> f64 {
        let p = alpha_val + beta_val * bp.mid;
        trace!("predict {:?}", p);
        p
    }
}

#[cfg(test)]
mod test {
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use itertools::Itertools;
    use plotters::prelude::*;
    use serde::{Deserialize, Serialize};
    use std::io::Result;

    use super::input::{read_csv, CsvRecord};
    use super::state::MovingState;
    use super::{BookPosition, DataRow, DataTable, Strategy};
    use ordered_float::OrderedFloat;
    use std::error::Error;
    use std::fs::File;
    use std::ops::Deref;
    use std::path::Path;
    use std::process::Command;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::runtime::Runtime;
    use util::date::{DateRange, DurationRangeType};
    use util::serdes::date_time_format;

    const LEFT_PAIR: &'static str = "ETH_USDT";
    const RIGHT_PAIR: &'static str = "BTC_USDT";

    #[derive(Debug, Serialize, Deserialize)]
    struct StrategyLog {
        time: DateTime<Utc>,
        right_mid: f64,
        left_mid: f64,
        state: MovingState,
    }

    impl StrategyLog {
        fn from_state(time: DateTime<Utc>, state: MovingState, last_row: &DataRow) -> StrategyLog {
            StrategyLog {
                time,
                right_mid: last_row.right.mid,
                left_mid: last_row.left.mid,
                state,
            }
        }
    }

    fn to_pos(r: &CsvRecord) -> BookPosition {
        BookPosition::new(r.a1, r.aq1, r.b1, r.bq1)
    }

    fn load_records(path: &str) -> Vec<CsvRecord> {
        let dt1 = read_csv(path).unwrap();
        dt1
    }

    fn load_csv_dataset(dr: &DateRange) -> (Vec<CsvRecord>, Vec<CsvRecord>) {
        let bp = std::env::var_os("BITCOINS_REPO")
            .and_then(|oss| oss.into_string().ok())
            .unwrap_or("..".to_string());
        let exchange_name = "Binance";
        let channel = "order_books";

        let base_path = Path::new(&bp)
            .join("data")
            .join(exchange_name)
            .join(channel);
        let bpc = Arc::new(bp.clone());
        let mut rt = Runtime::new().unwrap();
        let mut dl_test_data = |pair: &'static str| {
            rt.block_on(async {
                let out_file_name = format!("{}.zip", pair);
                let file = tempfile::tempdir().unwrap();
                let out_file = file.into_path().join(out_file_name);
                let s3_key = &format!("test_data/{}/{}/{}.zip", exchange_name, channel, pair);
                util::s3::download_file(&s3_key.clone(), out_file.clone())
                    .await
                    .unwrap();
                let bp = bpc.deref();

                Command::new("unzip")
                    .arg(&out_file)
                    .arg("-d")
                    .arg(bp)
                    .output()
                    .expect("failed to unzip file");
            });
        };
        for s in vec![LEFT_PAIR, RIGHT_PAIR] {
            if !base_path.exists() || !base_path.join(&format!("pr={}", s)).exists() {
                //download dataset from spaces
                std::fs::create_dir_all(&base_path);
                dl_test_data(s);
            }
        }

        let get_records = move |p: String| {
            dr.clone()
                .flat_map(|dt| {
                    let date = dt.clone();
                    load_records(
                        base_path
                            .join(format!("pr={}", p.clone()))
                            .join(format!("dt={}.csv", date.format("%Y-%m-%d")))
                            .to_str()
                            .unwrap(),
                    )
                })
                .collect()
        };
        return (
            get_records(LEFT_PAIR.to_string()),
            get_records(RIGHT_PAIR.to_string()),
        );
    }

    fn draw_line_plot(data: Vec<StrategyLog>) -> std::result::Result<String, Box<dyn Error>> {
        let now = Utc::now();
        let string = format!(
            "graphs/naive_pair_trading_plot_{}.svg",
            now.format("%Y%m%d%H:%M:%S")
        );
        let color_wheel = vec![&BLACK, &BLUE, &RED];
        let more_lines: Vec<(&str, Vec<fn(&StrategyLog) -> f64>)> = vec![
            (
                "value",
                vec![|x| x.right_mid, |x| x.state.predicted_right()],
            ),
            (
                "return",
                vec![|x| x.state.short_position_return() + x.state.long_position_return()],
            ),
            ("PnL", vec![|x| x.state.pnl()]),
            ("Nominal Position", vec![|x| x.state.nominal_position()]),
            ("Beta", vec![|x| x.state.beta_lr()]),
            ("res", vec![|x| x.state.res()]),
            ("traded_price_left", vec![|x| x.state.traded_price_left()]),
            ("traded_price_right", vec![|x| x.state.traded_price_right()]),
            ("alpha_val", vec![|x| x.state.alpha()]),
            ("value_strat", vec![|x| x.state.value_strat()]),
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
                .build_ranged(x_range.clone(), y_range)?;
            chart.configure_mesh().line_style_2(&WHITE).draw()?;
            for (j, line_spec) in line_specs.1.iter().enumerate() {
                chart.draw_series(LineSeries::new(
                    skipped_data.clone().map(|x| (x.time, line_spec(x))),
                    color_wheel[j],
                ))?;
            }
        }

        Ok(string.clone())
    }

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn beta_val() {
        init();
        let mut dt = DataTable {
            rows: Vec::new(),
            window_size: 500,
            max_size: 2000,
        };
        // Read downsampled streams
        let dt0 = Utc.ymd(2020, 03, 25);
        let dt1 = Utc.ymd(2020, 03, 25);
        let (left_records, right_records) =
            load_csv_dataset(&DateRange(dt0, dt1, DurationRangeType::Days, 1));
        // align data
        left_records
            .iter()
            .zip(right_records.iter())
            .take(500)
            .for_each(|(l, r)| {
                dt.push(&DataRow {
                    time: l.hourofday,
                    left: to_pos(l),
                    right: to_pos(r),
                })
            });
        let x = dt.beta();
        println!("beta {}", x);
        assert!(x > 0.0, x);
    }

    #[test]
    fn continuous_scenario() {
        init();
        let mut strat = Strategy::new(LEFT_PAIR, RIGHT_PAIR, 5000, Duration::minutes(1));
        // Read downsampled streams
        let dt0 = Utc.ymd(2020, 03, 25);
        let dt1 = Utc.ymd(2020, 04, 08);
        let (left_records, right_records) =
            load_csv_dataset(&DateRange(dt0, dt1, DurationRangeType::Days, 1));
        println!("Dataset loaded in memory...");
        // align data
        let mut elapsed = 0 as u128;
        let mut iterations = 0 as u128;
        let (zip, other) = left_records.iter().zip(right_records.iter()).tee();
        let (left, right) = other.tee();
        let left_sum: f64 = left.map(|r| (r.0.a1 + r.0.b1) / 2.0).sum();
        let right_sum: f64 = right.map(|r| (r.1.a1 + r.1.b1) / 2.0).sum();
        println!("crypto1_m {}", left_sum);
        println!("crypto2_m {}", right_sum);
        let logs: Vec<StrategyLog> = zip
            .enumerate()
            .map(|(i, (l, r))| {
                iterations += 1;
                let now = Instant::now();

                if i % 1000 == 0 {
                    trace!(
                        "{} iterations..., table of size {}",
                        i,
                        strat.data_table.rows.len()
                    );
                }
                let log = {
                    let row_time = l.hourofday;
                    let row = DataRow {
                        time: row_time,
                        left: to_pos(l),
                        right: to_pos(r),
                    };
                    strat.process_row(&row);
                    StrategyLog::from_state(row_time, strat.state.clone(), &row)
                };
                elapsed += now.elapsed().as_nanos();
                log
            })
            .collect();
        println!("Each iteration took {} on avg", elapsed / iterations);

        let mut positions = strat.get_positions();
        positions.sort_by(|p1, p2| p1.time.cmp(&p2.time));
        assert_eq!(
            Some(161.270004272461),
            positions.last().map(|p| p.left_price)
        );

        // let logs_f = std::fs::File::create("strategy_logs.json").unwrap();
        // serde_json::to_writer(logs_f, &logs);
        std::fs::create_dir_all("graphs");
        let drew = draw_line_plot(logs);
        if let Ok(file) = drew {
            let copied = std::fs::copy(&file, "graphs/naive_pair_trading_plot_latest.svg");
            assert!(copied.is_ok(), format!("{:?}", copied));
        } else {
            assert!(false, format!("{:?}", drew));
        }
    }
}
