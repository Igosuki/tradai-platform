use anyhow::Result;
use chrono::{DateTime, Duration, TimeZone, Utc};
use log::Level::Debug;
use std::convert::TryInto;
use std::ops::{Add, Mul, Sub};
use std::sync::Arc;

mod covar_model;
pub mod input;
pub mod metrics;
pub mod options;
pub mod state;

use crate::model::PositionKind;
use crate::naive_pair_trading::covar_model::DataRow;
use crate::naive_pair_trading::state::Operation;
use crate::ob_linear_model::{BookPosition, LinearModelTable};
use crate::order_manager::OrderManager;
use crate::query::{FieldMutation, MutableField};
use crate::{DataQuery, DataResult, StrategyInterface};
use actix::Addr;
use coinnect_rt::types::LiveEvent;
use db::Db;
use metrics::NaiveStrategyMetrics;
use options::Options;
use state::{MovingState, Position};

const LM_AGE_CUTOFF_RATIO: f64 = 0.0013;

pub struct NaiveTradingStrategy {
    fees_rate: f64,
    res_threshold_long: f64,
    res_threshold_short: f64,
    stop_loss: f64,
    stop_gain: f64,
    beta_eval_window_size: i32,
    beta_eval_freq: i32,
    beta_sample_freq: Duration,
    state: MovingState,
    data_table: LinearModelTable<DataRow>,
    pub right_pair: String,
    pub left_pair: String,
    metrics: Arc<NaiveStrategyMetrics>,
    last_row_process_time: DateTime<Utc>,
    last_sample_time: DateTime<Utc>,
    last_row_time_at_eval: DateTime<Utc>,
    last_left: Option<BookPosition>,
    last_right: Option<BookPosition>,
}

impl NaiveTradingStrategy {
    pub fn new(db_path: &str, fees_rate: f64, n: &Options, om: Addr<OrderManager>) -> Self {
        let metrics =
            NaiveStrategyMetrics::for_strat(prometheus::default_registry(), &n.left, &n.right);
        let strat_db_path = format!("{}/naive_pair_trading_{}_{}", db_path, n.left, n.right);
        let db_name = format!("{}_{}", n.left, n.right);
        let db = Db::new(&strat_db_path, db_name);
        Self {
            fees_rate,
            res_threshold_long: n.threshold_long,
            res_threshold_short: n.threshold_short,
            stop_loss: n.stop_loss,
            stop_gain: n.stop_gain,
            beta_eval_window_size: n.window_size,
            beta_eval_freq: n.beta_eval_freq,
            state: MovingState::new(n.initial_cap, db, om, n.dry_mode()),
            data_table: Self::make_lm_table(
                &n.left,
                &n.right,
                &strat_db_path,
                n.window_size as usize,
            ),
            right_pair: n.right.to_string(),
            left_pair: n.left.to_string(),
            last_row_process_time: Utc.timestamp_millis(0),
            last_sample_time: Utc.timestamp_millis(0),
            last_row_time_at_eval: Utc.timestamp_millis(0),
            metrics: Arc::new(metrics),
            last_left: None,
            last_right: None,
            beta_sample_freq: n.beta_sample_freq(),
        }
    }

    pub fn make_lm_table(
        left_pair: &str,
        right_pair: &str,
        db_path: &str,
        window_size: usize,
    ) -> LinearModelTable<DataRow> {
        LinearModelTable::new(
            &format!("{}_{}", left_pair, right_pair),
            db_path,
            window_size,
            Box::new(covar_model::beta),
            Box::new(covar_model::alpha),
        )
    }

    fn maybe_log_stop_loss(&self, pk: PositionKind) {
        if self.should_stop(&pk) {
            let ret = self.return_value(&pk);
            let expr = if ret > self.stop_gain {
                "gain"
            } else if ret < self.stop_loss {
                "loss"
            } else {
                "n/a"
            };
            info!(
                "---- Stop-{} executed ({} position) ----",
                expr,
                match pk {
                    PositionKind::SHORT => "short",
                    PositionKind::LONG => "long",
                },
            )
        }
    }

    fn should_eval(&self, event_time: DateTime<Utc>) -> bool {
        let model_time = self.last_row_time_at_eval;
        let mt_obsolescence = if self.state.beta_lr() < 0.0 {
            // When beta is negative the evaluation frequency is ten times lower
            model_time.add(self.beta_sample_freq.mul(self.beta_eval_freq / 10))
        } else {
            // Model obsolescence is defined here as event time being greater than the sample window
            model_time.add(self.beta_sample_freq.mul(self.beta_eval_freq))
        };
        let is_model_obsolete = event_time.ge(&mt_obsolescence);
        if is_model_obsolete && log_enabled!(Debug) {
            debug!(
                "model obsolete, eval time reached : {} > {} with model_time = {}, beta_val = {}",
                event_time,
                mt_obsolescence,
                model_time,
                self.state.beta_lr()
            );
        }
        is_model_obsolete
    }

    fn set_model_from_table(&mut self) {
        let lmb = self.data_table.model();
        lmb.map(|lm| {
            self.state.set_beta(lm.beta);
            self.state.set_alpha(lm.alpha);
        });
    }

    fn eval_linear_model(&mut self) {
        if let Err(e) = self.data_table.update_model() {
            error!("Error saving model : {:?}", e);
        }
        self.set_model_from_table();
        self.last_row_time_at_eval = self.last_sample_time;
    }

    fn predict(&self, bp: &BookPosition) -> f64 {
        self.data_table
            .predict(self.state.alpha(), self.state.beta(), bp)
    }

    fn update_spread(&mut self, row: &DataRow) {
        self.set_long_spread(row.right.ask);
        self.set_short_spread(row.left.ask);
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

    fn should_stop(&self, pk: &PositionKind) -> bool {
        let ret = self.return_value(pk);
        ret > self.stop_gain || ret < self.stop_loss
    }

    fn return_value(&self, pk: &PositionKind) -> f64 {
        match pk {
            PositionKind::SHORT => self.state.short_position_return(),
            PositionKind::LONG => self.state.long_position_return(),
        }
    }

    async fn eval_latest(&mut self, lr: &DataRow) {
        if self.state.no_position_taken() {
            if self.should_eval(lr.time) {
                self.eval_linear_model();
            }
            self.update_spread(lr);
        }

        self.state.set_beta_lr();
        self.state.set_predicted_right(self.predict(&lr.left));
        self.state
            .set_res((lr.right.mid - self.state.predicted_right()) / lr.right.mid);

        // If a position is taken, resolve pending operations
        // In case of error return immediately as no trades can be made until the position is resolved
        if self
            .state
            .resolve_pending_operations(&lr.left, &lr.right)
            .await
            .is_err()
        {
            return;
        }

        if self.state.beta_lr() <= 0.0 {
            return;
        }

        // Possibly open a short position
        if (self.state.res() > self.res_threshold_short) && self.state.no_position_taken() {
            let position = self.short_position(lr.right.bid, lr.left.ask, lr.time);
            let op = self.state.open(position, self.fees_rate).await;
            self.metrics.log_position(&op.pos, &op.kind);
        }

        // Possibly close a short position
        if self.state.is_short() {
            self.state
                .set_short_position_return(self.fees_rate, lr.right.ask, lr.left.bid);
            if (self.state.res() <= self.res_threshold_short && self.state.res() < 0.0)
                || self.should_stop(&PositionKind::SHORT)
            {
                self.maybe_log_stop_loss(PositionKind::SHORT);
                let position = self.short_position(lr.right.ask, lr.left.bid, lr.time);
                let op = self.state.close(position, self.fees_rate).await;
                self.metrics.log_position(&op.pos, &op.kind);
                self.eval_linear_model();
            }
        }

        // Possibly open a long position
        if self.state.res() <= self.res_threshold_long && self.state.no_position_taken() {
            let position = self.long_position(lr.right.ask, lr.left.bid, lr.time);
            let op = self.state.open(position, self.fees_rate).await;
            self.metrics.log_position(&op.pos, &op.kind);
        }

        // Possibly close a long position
        if self.state.is_long() {
            self.state
                .set_long_position_return(self.fees_rate, lr.right.bid, lr.left.ask);
            if (self.state.res() >= self.res_threshold_long && self.state.res() > 0.0)
                || self.should_stop(&PositionKind::LONG)
            {
                self.maybe_log_stop_loss(PositionKind::LONG);
                let position = self.long_position(lr.right.bid, lr.left.ask, lr.time);
                let op = self.state.close(position, self.fees_rate).await;
                self.metrics.log_position(&op.pos, &op.kind);
                self.eval_linear_model();
            }
        }
    }

    fn can_eval(&self) -> bool {
        self.data_table.has_model()
            && (!self.state.no_position_taken()
                || self
                    .data_table
                    .model()
                    .map(|m| {
                        m.at.gt(&Utc::now().sub(self.beta_sample_freq.mul(
                            (self.beta_eval_freq as f64 * (1.0 + LM_AGE_CUTOFF_RATIO)) as i32,
                        )))
                    })
                    .unwrap_or(false))
    }

    async fn process_row(&mut self, row: &DataRow) {
        if self.data_table.try_loading_model() {
            self.set_model_from_table();
            self.last_row_time_at_eval = self
                .data_table
                .last_model_time()
                .unwrap_or_else(|| Utc.timestamp_millis(0));
        }
        let time = self.last_sample_time.add(self.beta_sample_freq);
        let should_sample = row.time.gt(&time) || row.time == time;
        // A model is available
        let can_eval = self.can_eval();
        if should_sample {
            self.last_sample_time = row.time;
        }
        if can_eval {
            self.eval_latest(row).await;
            self.log_state();
        }
        self.metrics.log_row(&row);
        if should_sample {
            self.data_table.push(row);
        }

        // No model and there are enough samples
        if !can_eval && self.data_table.len() >= self.beta_eval_window_size as usize {
            self.eval_linear_model();
            self.update_spread(row);
        }
    }

    fn log_state(&self) {
        self.metrics.log_state(&self.state);
    }

    fn get_operations(&self) -> Vec<Operation> {
        self.state.get_operations()
    }

    fn get_ongoing_op(&self) -> &Option<Operation> {
        self.state.ongoing_op()
    }

    fn dump_db(&self) -> Vec<String> {
        self.state.dump_db()
    }

    fn change_state(&mut self, field: MutableField, v: f64) -> Result<()> {
        self.state.change_state(field, v)
    }
}

#[async_trait]
impl StrategyInterface for NaiveTradingStrategy {
    async fn add_event(&mut self, le: LiveEvent) -> anyhow::Result<()> {
        if let LiveEvent::LiveOrderbook(ob) = le {
            let string = ob.pair.clone();
            if string == self.left_pair {
                self.last_left = ob.try_into().ok();
            } else if string == self.right_pair {
                self.last_right = ob.try_into().ok();
            }
        }
        let now = Utc::now();
        if now.gt(&self
            .last_row_process_time
            .add(chrono::Duration::milliseconds(200)))
        {
            if let (Some(l), Some(r)) = (self.last_left.clone(), self.last_right.clone()) {
                let x = DataRow {
                    left: l,
                    right: r,
                    time: now,
                };
                self.process_row(&x).await;
                self.last_row_process_time = now;
            }
        }
        Ok(())
    }

    fn data(&self, q: DataQuery) -> Option<DataResult> {
        match q {
            DataQuery::Operations => Some(DataResult::Operations(self.get_operations())),
            DataQuery::Dump => Some(DataResult::Dump(self.dump_db())),
            DataQuery::CurrentOperation => {
                Some(DataResult::Operation(self.get_ongoing_op().clone()))
            }
        }
    }

    fn mutate(&mut self, m: FieldMutation) -> Result<()> {
        self.change_state(m.field, m.value)
    }
}

#[cfg(test)]
mod test {
    use chrono::{DateTime, TimeZone, Utc};
    use itertools::Itertools;
    use plotters::prelude::*;
    use serde::Serialize;

    use super::input::CsvRecord;
    use super::state::MovingState;
    use super::{DataRow, NaiveTradingStrategy};
    use crate::naive_pair_trading::input::to_pos;
    use crate::naive_pair_trading::options::Options;
    use crate::order_manager::OrderManager;
    use actix::Actor;
    use coinnect_rt::exchange::{Exchange, ExchangeApi, MockApi};
    use ordered_float::OrderedFloat;
    use std::error::Error;
    use std::ops::Deref;
    use std::path::Path;
    use std::process::Command;
    use std::sync::Arc;
    use std::time::Instant;
    use util::date::{DateRange, DurationRangeType};

    static LEFT_PAIR: &str = "ETH_USDT";
    static RIGHT_PAIR: &str = "BTC_USDT";

    #[derive(Debug, Serialize)]
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

    async fn load_csv_dataset(dr: &DateRange) -> (Vec<CsvRecord>, Vec<CsvRecord>) {
        let bp = std::env::var_os("BITCOINS_REPO")
            .and_then(|oss| oss.into_string().ok())
            .unwrap_or_else(|| "..".to_string());
        let exchange_name = "Binance";
        let channel = "order_books";

        let base_path = Path::new(&bp)
            .join("data")
            .join(exchange_name)
            .join(channel);
        let bpc = Arc::new(bp);
        let dl_test_data = async move |bpc: Arc<String>, pair: &'static str| {
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
        };
        for s in &[LEFT_PAIR, RIGHT_PAIR] {
            if !base_path.exists() || !base_path.join(&format!("pr={}", s)).exists() {
                //download dataset from spaces
                std::fs::create_dir_all(&base_path).unwrap();
                dl_test_data(bpc.clone(), s).await;
            }
        }
        super::input::load_records_from_csv(dr, &base_path, LEFT_PAIR, RIGHT_PAIR, "*csv")
    }

    type StrategyEntry<'a> = (&'a str, Vec<fn(&StrategyLog) -> f64>);

    fn draw_line_plot(data: Vec<StrategyLog>) -> std::result::Result<String, Box<dyn Error>> {
        let now = Utc::now();
        let string = format!(
            "graphs/naive_pair_trading_plot_{}.svg",
            now.format("%Y%m%d%H:%M:%S")
        );
        let color_wheel = vec![&BLACK, &BLUE, &RED];
        let more_lines: Vec<StrategyEntry<'_>> = vec![
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

    #[tokio::test]
    async fn beta_val() {
        init();
        let mut dt = NaiveTradingStrategy::make_lm_table("BTC_USDT", "ETH_USDT", "default", 500);
        // Read downsampled streams
        let dt0 = Utc.ymd(2020, 3, 25);
        let dt1 = Utc.ymd(2020, 3, 25);
        let (left_records, right_records) =
            load_csv_dataset(&DateRange(dt0, dt1, DurationRangeType::Days, 1)).await;
        // align data
        left_records
            .iter()
            .zip(right_records.iter())
            .take(500)
            .for_each(|(l, r)| {
                dt.push(&DataRow {
                    time: l.event_ms,
                    left: to_pos(l),
                    right: to_pos(r),
                })
            });
        let x = dt.beta();
        println!("beta {}", x);
        assert!(x > 0.0, x);
    }

    #[actix_rt::test]
    async fn continuous_scenario() {
        init();
        let root = tempdir::TempDir::new("test_data2").unwrap();
        let beta_eval_freq = 1000;
        let window_size = 2000;
        let buf = root.into_path();
        let path = buf.to_str().unwrap();
        let capi: Box<dyn ExchangeApi> = Box::new(MockApi);
        let api = Arc::new(capi);
        let order_manager = OrderManager::new(api, Path::new(path));
        let order_manager_addr = OrderManager::start(order_manager);
        let mut strat = NaiveTradingStrategy::new(
            path,
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
        let (left_records, right_records) =
            load_csv_dataset(&DateRange(dt0, dt1, DurationRangeType::Days, 1)).await;
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
                    left: to_pos(l),
                    right: to_pos(r),
                };
                strat.process_row(&row).await;
                StrategyLog::from_state(row_time, strat.state.clone(), &row)
            };
            elapsed += now.elapsed().as_nanos();
            logs.push(log);
        }
        println!("Each iteration took {} on avg", elapsed / iterations);

        let mut positions = strat.get_operations();
        positions.sort_by(|p1, p2| p1.pos.time.cmp(&p2.pos.time));
        let last_position = positions.last();
        assert_eq!(
            Some(162.130004882813),
            last_position.map(|p| p.pos.left_price)
        );
        assert_eq!(
            Some(33.33032942489664),
            last_position.map(|p| p.left_value())
        );

        // let logs_f = std::fs::File::create("strategy_logs.json").unwrap();
        // serde_json::to_writer(logs_f, &logs);
        std::fs::create_dir_all("graphs").unwrap();
        let drew = draw_line_plot(logs);
        if let Ok(file) = drew {
            let copied = std::fs::copy(&file, "graphs/naive_pair_trading_plot_latest.svg");
            assert!(copied.is_ok(), format!("{:?}", copied));
        } else {
            panic!(format!("{:?}", drew));
        }
    }
}
