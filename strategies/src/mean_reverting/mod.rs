use actix::Addr;
use anyhow::Result;
use chrono::{DateTime, Duration, TimeZone, Utc};
use coinnect_rt::types::{LiveEvent, Pair};
use db::Db;
use itertools::Itertools;
use std::convert::TryInto;
use std::ops::{Add, Mul};
use std::path::PathBuf;

use crate::mean_reverting::ema_model::SinglePosRow;
use crate::mean_reverting::metrics::MeanRevertingStrategyMetrics;
use crate::mean_reverting::options::Options;
use crate::mean_reverting::state::{MeanRevertingState, Operation, Position};
use crate::model::PositionKind;
use crate::ob_double_window_model::DoubleWindowTable;
use crate::order_manager::OrderManager;
use crate::query::{DataQuery, DataResult, FieldMutation, MutableField};
use crate::StrategyInterface;
use math::iter::QuantileExt;
use ordered_float::OrderedFloat;
use std::cmp::{max, min};
use std::sync::Arc;

mod ema_model;
mod metrics;
mod options;
pub mod state;

#[derive(Clone, Serialize, Deserialize)]
pub struct PosAndApo {
    apo: f64,
    row: SinglePosRow,
}

pub struct MeanRevertingStrategy {
    pair: Pair,
    fees_rate: f64,
    sample_freq: Duration,
    last_sample_time: DateTime<Utc>,
    state: MeanRevertingState,
    data_table: DoubleWindowTable<PosAndApo>,
    last_row_time_at_eval: DateTime<Utc>,
    last_row_process_time: DateTime<Utc>,
    metrics: Arc<MeanRevertingStrategyMetrics>,
    threshold_eval_freq: Option<i32>,
    threshold_eval_window_size: Option<usize>,
    threshold_short_0: f64,
    threshold_long_0: f64,
    dynamic_threshold: bool,
    last_threshold_time: DateTime<Utc>,
    stop_loss: f64,
    stop_gain: f64,
    long_window_size: usize,
}

static MEAN_REVERTING_DB_KEY: &str = "mean_reverting";

impl MeanRevertingStrategy {
    pub fn new(db_path: &str, fees_rate: f64, n: &Options, om: Addr<OrderManager>) -> Self {
        let metrics =
            MeanRevertingStrategyMetrics::for_strat(prometheus::default_registry(), &n.pair);
        let mut pb = PathBuf::from(db_path);
        let strat_db_path = format!("{}_{}", MEAN_REVERTING_DB_KEY, n.pair);
        pb.push(strat_db_path);
        let db_name = format!("{}", n.pair);
        let db = Db::new(&pb.to_str().unwrap(), db_name);
        let state = MeanRevertingState::new(n, db, om);
        let dynamic_threshold_enabled = n.dynamic_threshold();
        let max_size = if dynamic_threshold_enabled {
            n.threshold_window_size
                .map(|t| max(t, 2 * n.long_window_size))
        } else {
            None
        };

        let data_table = Self::make_model_table(
            n.pair.as_ref(),
            pb.to_str().unwrap(),
            n.short_window_size,
            n.long_window_size,
            max_size,
        );
        Self {
            pair: n.pair.clone(),
            fees_rate,
            sample_freq: n.sample_freq(),
            last_sample_time: Utc.timestamp_millis(0),
            last_row_time_at_eval: Utc.timestamp_millis(0),
            last_row_process_time: Utc.timestamp_millis(0),
            state,
            data_table,
            threshold_eval_freq: n.threshold_eval_freq,
            threshold_eval_window_size: n.threshold_window_size,
            threshold_short_0: n.threshold_short,
            threshold_long_0: n.threshold_long,
            dynamic_threshold: dynamic_threshold_enabled,
            last_threshold_time: Utc.timestamp_millis(0),
            stop_loss: n.stop_loss,
            metrics: Arc::new(metrics),
            long_window_size: n.long_window_size,
            stop_gain: n.stop_gain,
        }
    }

    pub fn make_model_table(
        pair: &str,
        db_path: &str,
        short_window_size: usize,
        long_window_size: usize,
        max_size_factor: Option<usize>,
    ) -> DoubleWindowTable<PosAndApo> {
        DoubleWindowTable::new(
            &pair,
            db_path,
            short_window_size,
            long_window_size,
            max_size_factor,
            Box::new(ema_model::moving_average_apo),
        )
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

    fn set_model_from_table(&mut self) {
        let lmb = self.data_table.model();
        lmb.map(|lm| {
            self.state.set_apo(lm.value);
        });
    }

    fn eval_model(&mut self) {
        if let Err(e) = self.data_table.update_model() {
            error!("Error saving model : {:?}", e);
        }
        self.set_model_from_table();
        self.last_row_time_at_eval = self.last_sample_time;
    }

    fn short_position(&self, price: f64, time: DateTime<Utc>) -> Position {
        Position {
            kind: PositionKind::SHORT,
            price,
            time,
            pair: self.pair.to_string(),
        }
    }

    fn long_position(&self, price: f64, time: DateTime<Utc>) -> Position {
        Position {
            kind: PositionKind::LONG,
            price,
            time,
            pair: self.pair.to_string(),
        }
    }

    fn maybe_eval_threshold(&mut self, current_time: DateTime<Utc>) {
        if self.dynamic_threshold {
            if let (Some(threshold_eval_freq), Some(threshold_window_size)) =
                (self.threshold_eval_freq, self.threshold_eval_window_size)
            {
                // Threshold obsolescence is defined by sample_freq * threshold_eval_freq > now
                let obsolete_threshold_time = self
                    .last_threshold_time
                    .add(self.sample_freq.mul(threshold_eval_freq));
                if current_time.gt(&obsolete_threshold_time)
                    && self.data_table.len() > threshold_window_size
                {
                    let wdw = self.data_table.window(threshold_window_size);
                    let (threshold_short_iter, threshold_long_iter) = wdw.map(|r| r.apo).tee();
                    self.state.set_threshold_short(
                        max(
                            OrderedFloat(self.threshold_short_0),
                            OrderedFloat(threshold_short_iter.quantile(0.99)),
                        )
                        .into(),
                    );
                    self.state.set_threshold_long(
                        min(
                            OrderedFloat(self.threshold_long_0),
                            OrderedFloat(threshold_long_iter.quantile(0.01)),
                        )
                        .into(),
                    );
                    self.metrics.log_thresholds(&self.state);
                }
            }
        }
    }

    async fn eval_latest(&mut self, lr: &SinglePosRow) {
        self.eval_model();
        self.maybe_eval_threshold(lr.time);

        if self.state.no_position_taken() {
            self.state.update_units(&lr.pos, self.fees_rate);
        }

        // If a position is taken, resolve pending operations
        // In case of error return immediately as no trades can be made until the position is resolved
        if self
            .state
            .resolve_pending_operations(&lr.pos)
            .await
            .is_err()
        {
            return;
        }

        // Possibly open a short position
        if (self.state.apo() > self.state.threshold_short()) && self.state.no_position_taken() {
            info!(
                "Entering short position with threshold {}",
                self.state.threshold_short()
            );
            let position = self.short_position(lr.pos.bid, lr.time);
            let op = self.state.open(position, self.fees_rate).await;
            // self.metrics.log_position(&op.pos, &op.kind);
        }

        // Possibly close a short position
        if self.state.is_short() {
            self.state
                .set_short_position_return(self.fees_rate, lr.pos.ask);
            if (self.state.apo() < 0.0) || self.should_stop(&PositionKind::SHORT) {
                self.maybe_log_stop_loss(PositionKind::SHORT);
                let position = self.short_position(lr.pos.ask, lr.time);
                let op = self.state.close(position, self.fees_rate).await;
                // self.metrics.log_position(&op.pos, &op.kind);
            }
        }

        // Possibly open a long position
        if (self.state.apo() > self.state.threshold_long()) && self.state.no_position_taken() {
            info!(
                "Entering long position with threshold {}",
                self.state.threshold_long()
            );
            let position = self.long_position(lr.pos.ask, lr.time);
            let op = self.state.open(position, self.fees_rate).await;
            // self.metrics.log_position(&op.pos, &op.kind);
        }

        // Possibly close a long position
        if self.state.is_long() {
            self.state
                .set_long_position_return(self.fees_rate, lr.pos.bid);
            if (self.state.apo() < 0.0) || self.should_stop(&PositionKind::LONG) {
                self.maybe_log_stop_loss(PositionKind::LONG);
                let position = self.long_position(lr.pos.bid, lr.time);
                let op = self.state.close(position, self.fees_rate).await;
                // self.metrics.log_position(&op.pos, &op.kind);
            }
        }
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

    fn return_value(&self, pk: &PositionKind) -> f64 {
        match pk {
            PositionKind::SHORT => self.state.short_position_return(),
            PositionKind::LONG => self.state.long_position_return(),
        }
    }

    fn should_stop(&self, pk: &PositionKind) -> bool {
        let ret = self.return_value(pk);
        ret > self.stop_gain || ret < self.stop_loss
    }

    fn can_eval(&self) -> bool {
        self.data_table.has_model()
    }

    async fn process_row(&mut self, row: &SinglePosRow) {
        if self.data_table.try_loading_model() {
            self.set_model_from_table();
            self.last_row_time_at_eval = self
                .data_table
                .last_model_time()
                .unwrap_or_else(|| Utc.timestamp_millis(0));
        }
        let time = self.last_sample_time.add(self.sample_freq);
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
        // self.metrics.log_row(&row);
        if should_sample {
            self.data_table.push(&PosAndApo {
                row: row.clone(),
                apo: self.state.apo(),
            });
        }

        // No model and there are enough samples
        if !can_eval && self.data_table.len() >= self.long_window_size as usize {
            self.eval_model();
        }
    }
}

#[async_trait]
impl StrategyInterface for MeanRevertingStrategy {
    async fn add_event(&mut self, le: LiveEvent) -> anyhow::Result<()> {
        if let LiveEvent::LiveOrderbook(ob) = le {
            let book_pos = ob.try_into().ok();
            if let Some(pos) = book_pos {
                let now = Utc::now();
                let x = SinglePosRow { time: now, pos };
                self.process_row(&x).await;
                self.last_row_process_time = now;
            }
        }
        Ok(())
    }

    fn data(&self, q: DataQuery) -> Option<DataResult> {
        match q {
            DataQuery::Operations => {
                Some(DataResult::MeanRevertingOperations(self.get_operations()))
            }
            DataQuery::Dump => Some(DataResult::Dump(self.dump_db())),
            DataQuery::CurrentOperation => Some(DataResult::MeanRevertingOperation(
                self.get_ongoing_op().clone(),
            )),
        }
    }

    fn mutate(&mut self, m: FieldMutation) -> Result<()> {
        self.change_state(m.field, m.value)
    }
}

#[cfg(test)]
mod test {
    use chrono::{DateTime, TimeZone, Utc};
    use plotters::prelude::*;
    use std::error::Error;
    use std::time::Instant;
    use util::date::{DateRange, DurationRangeType};

    use crate::input::{self, to_pos};
    use crate::mean_reverting::ema_model::SinglePosRow;
    use crate::mean_reverting::options::Options;
    use crate::mean_reverting::state::MeanRevertingState;
    use crate::mean_reverting::{MeanRevertingStrategy, PosAndApo};
    use crate::order_manager::test_util;
    use ordered_float::OrderedFloat;

    #[derive(Debug, Serialize)]
    struct StrategyLog {
        time: DateTime<Utc>,
        mid: f64,
        state: MeanRevertingState,
    }

    impl StrategyLog {
        fn from_state(
            time: DateTime<Utc>,
            state: MeanRevertingState,
            last_row: &SinglePosRow,
        ) -> StrategyLog {
            StrategyLog {
                time,
                mid: last_row.pos.mid,
                state,
            }
        }
    }

    fn draw_line_plot(data: Vec<StrategyLog>) -> std::result::Result<String, Box<dyn Error>> {
        let now = Utc::now();
        let string = format!(
            "graphs/mean_reverting_plot_{}.svg",
            now.format("%Y%m%d%H:%M:%S")
        );
        let color_wheel = vec![&BLACK, &BLUE, &RED];
        let more_lines: Vec<StrategyEntry<'_>> = vec![
            (
                "return",
                vec![|x| x.state.short_position_return() + x.state.long_position_return()],
            ),
            ("PnL", vec![|x| x.state.pnl()]),
            ("Nominal Position", vec![|x| x.state.nominal_position()]),
            ("apo", vec![|x| x.state.apo()]),
            ("traded_left", vec![|x| x.state.traded_price()]),
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
        let mut dt =
            MeanRevertingStrategy::make_model_table("BTC_USDT", "default", 100, 1000, None);
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
                dt.push(&PosAndApo {
                    apo: 0.0,
                    row: SinglePosRow {
                        time: l.event_ms,
                        pos: to_pos(r),
                    },
                })
            });
        dt.update_model().unwrap();
        let model_value = dt.model().unwrap().value;
        println!("beta {}", model_value);
        assert!(model_value > 0.0, model_value);
    }

    #[actix_rt::test]
    async fn continuous_scenario() {
        init();
        let root = tempdir::TempDir::new("test_data2").unwrap();
        let window_size = 1000;
        let buf = root.into_path();
        let path = buf.to_str().unwrap();
        let order_manager_addr = test_util::mock_manager(path);
        let mut strat = MeanRevertingStrategy::new(
            path,
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
                short_window_size: window_size / 10,
                long_window_size: window_size,
                sample_freq: "1min".to_string(),
            },
            order_manager_addr,
        );
        // Read downsampled streams
        let dt0 = Utc.ymd(2020, 3, 25);
        let dt1 = Utc.ymd(2020, 4, 8);
        // align data
        let mut elapsed = 0 as u128;
        let mut iterations = 0 as u128;
        let records = input::load_csv_dataset(
            &DateRange(dt0, dt1, DurationRangeType::Days, 1),
            vec![PAIR.to_string()],
            EXCHANGE,
            CHANNEL,
        )
        .await;
        println!("Dataset loaded in memory...");
        // align data
        let eval = records[0].iter();
        let mut logs: Vec<StrategyLog> = Vec::new();
        for csvr in eval {
            iterations += 1;
            let now = Instant::now();

            let log = {
                let row_time = csvr.event_ms;
                let row = SinglePosRow {
                    time: row_time,
                    pos: to_pos(csvr),
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
        assert_eq!(Some(162.130004882813), last_position.map(|p| p.pos.price));
        assert_eq!(Some(33.33032942489664), last_position.map(|p| p.value()));

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
