use chrono::{DateTime, Utc};
use coinnect_rt::types::LiveEvent;
use itertools::Itertools;
use serde::export::Formatter;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::Arc;

use crate::db::Db;
use crate::math::iter::{CovarianceExt, MeanExt, VarianceExt};
use crate::strategies::metrics::StrategyMetrics;
use crate::strategies::StrategySink;

const TS_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

#[derive(Deserialize, Serialize)]
pub(super) struct Position {
    pub kind: PositionKind,
    pub right_price: f64,
    pub left_price: f64,
    pub time: DateTime<Utc>,
    pub right_pair: String,
    pub left_pair: String,
}

#[derive(Deserialize, Serialize)]
pub(super) enum PositionKind {
    SHORT,
    LONG,
}

impl Display for PositionKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            PositionKind::SHORT => "short",
            PositionKind::LONG => "long",
        })
    }
}

pub enum Operation {
    OPEN,
    CLOSE,
    BUY,
    SELL,
}

impl Display for Operation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Operation::OPEN => "Open",
            Operation::CLOSE => "Close",
            Operation::BUY => "Buy",
            Operation::SELL => "Sell",
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MovingState {
    position_short: i8,
    position_long: i8,
    value_strat: f64,
    units_to_buy_long_spread: f64,
    units_to_buy_short_spread: f64,
    beta_val: f64,
    alpha_val: f64,
    beta_lr: f64,
    pub predicted_right: f64,
    res: f64,
    open_position: f64,
    nominal_position: f64,
    traded_price_right: f64,
    traded_price_left: f64,
    short_position_return: f64,
    long_position_return: f64,
    close_position: f64,
    pnl: f64,
}

impl MovingState {
    fn new(initial_value: f64) -> MovingState {
        MovingState {
            position_short: 0,
            position_long: 0,
            value_strat: initial_value,
            units_to_buy_long_spread: 0.0,
            units_to_buy_short_spread: 0.0,
            beta_val: 0.0,
            alpha_val: 0.0,
            beta_lr: 0.0,
            predicted_right: 0.0,
            res: 0.0,
            open_position: 0.0,
            nominal_position: 0.0,
            traded_price_right: 0.0,
            traded_price_left: 0.0,
            short_position_return: 0.0,
            long_position_return: 0.0,
            close_position: 0.0,
            pnl: 0.0,
        }
    }

    fn log_pos(&self, op: &Operation, pos: &PositionKind, time: DateTime<Utc>) {
        debug!(
            "{} {} position at {}",
            op,
            match pos {
                PositionKind::SHORT => "short",
                PositionKind::LONG => "long",
            },
            time.format(TS_FORMAT)
        );
    }

    fn log_trade(&self, op: Operation, spread: f64, pair: &str, value: f64, qty: f64) {
        debug!("{} {:.2} {} at {} for {:.2}", op, spread, pair, value, qty);
    }

    fn log_info(&self, pos: &PositionKind) {
        debug!(
            "Additional info : units {:.2} beta val {:.2} value strat {}",
            match pos {
                PositionKind::SHORT => self.units_to_buy_short_spread,
                PositionKind::LONG => self.units_to_buy_long_spread,
            },
            self.beta_val,
            self.value_strat
        );
        debug!("--------------------------------")
    }

    fn no_position_taken(&self) -> bool {
        self.position_short == 0 && self.position_long == 0
    }

    fn is_long(&self) -> bool {
        self.position_long == 1
    }

    fn is_short(&self) -> bool {
        self.position_short == 1
    }

    fn set_position_short(&mut self) {
        self.position_short = 1;
    }

    fn unset_position_short(&mut self) {
        self.position_short = 0;
    }

    fn unset_position_long(&mut self) {
        self.position_long = 0;
    }

    fn set_position_long(&mut self) {
        self.position_long = 1;
    }

    fn set_pnl(&mut self) {
        self.pnl = self.value_strat;
    }

    fn set_beta_lr(&mut self) {
        self.beta_lr = self.beta_val;
    }

    fn get_long_position_value(
        &self,
        fees_rate: f64,
        current_price_right: f64,
        current_price_left: f64,
    ) -> f64 {
        return self.units_to_buy_long_spread
            * (self.traded_price_left * self.beta_val * (1.0 - fees_rate)
                - self.traded_price_right * (1.0 + fees_rate)
                + current_price_right * (1.0 - fees_rate)
                - current_price_left * self.beta_val * (1.0 + fees_rate));
    }

    fn set_long_position_return(
        &mut self,
        fees_rate: f64,
        current_price_right: f64,
        current_price_left: f64,
    ) -> f64 {
        let x = self.get_long_position_value(fees_rate, current_price_right, current_price_left)
            / self.pnl;
        self.long_position_return = x;
        x
    }

    fn get_short_position_value(
        &self,
        fees_rate: f64,
        current_price_right: f64,
        current_price_left: f64,
    ) -> f64 {
        return self.units_to_buy_short_spread
            * (self.traded_price_right * (1.0 - fees_rate)
                - self.traded_price_left * self.beta_val * (1.0 + fees_rate)
                + current_price_left * self.beta_val * (1.0 - fees_rate)
                - current_price_right * (1.0 + fees_rate));
    }

    fn set_short_position_return(
        &mut self,
        fees_rate: f64,
        current_price_right: f64,
        current_price_left: f64,
    ) -> f64 {
        let x = self.get_short_position_value(fees_rate, current_price_right, current_price_left)
            / self.pnl;
        self.short_position_return = x;
        return x;
    }

    fn log_position(
        &self,
        pos: &Position,
        op: &Operation,
        spread: f64,
        right_coef: f64,
        left_coef: f64,
    ) {
        self.log_pos(op, &pos.kind, pos.time);
        let (left_op, right_op) = match (&pos.kind, &op) {
            (PositionKind::SHORT, Operation::OPEN) => (Operation::BUY, Operation::SELL),
            (PositionKind::LONG, Operation::OPEN) => (Operation::SELL, Operation::BUY),
            (PositionKind::SHORT, Operation::CLOSE) => (Operation::SELL, Operation::BUY),
            (PositionKind::LONG, Operation::CLOSE) => (Operation::BUY, Operation::SELL),
            _ => unimplemented!(),
        };
        self.log_trade(
            right_op,
            spread,
            &pos.right_pair,
            pos.right_price,
            spread * pos.right_price * right_coef.abs(),
        );
        self.log_trade(
            left_op,
            spread * self.beta_val,
            &pos.left_pair,
            pos.left_price,
            spread * pos.left_price * left_coef.abs(),
        );
        self.log_info(&pos.kind);
    }

    fn open(&mut self, pos: Position, fees: f64) {
        match pos.kind {
            PositionKind::SHORT => self.set_position_short(),
            PositionKind::LONG => self.set_position_long(),
        };
        self.open_position = 1e5;
        self.nominal_position = self.beta_val;
        self.traded_price_right = pos.right_price;
        self.traded_price_left = pos.left_price;
        let (spread, right_coef, left_coef) = match pos.kind {
            PositionKind::SHORT => (
                self.units_to_buy_short_spread,
                1.0 - fees,
                -(self.beta_val * (1.0 + fees)),
            ),
            PositionKind::LONG => (
                self.units_to_buy_long_spread,
                -(1.0 + fees),
                self.beta_val * (1.0 - fees),
            ),
        };
        self.value_strat += spread * (pos.right_price * right_coef + pos.left_price * left_coef);
        self.log_position(&pos, &Operation::OPEN, spread, right_coef, left_coef);
    }

    fn close(&mut self, pos: Position, fees: f64) {
        match pos.kind {
            PositionKind::SHORT => self.unset_position_short(),
            PositionKind::LONG => self.unset_position_long(),
        };
        self.close_position = 1e5;
        let (spread, right_coef, left_coef) = match pos.kind {
            PositionKind::SHORT => (
                self.units_to_buy_short_spread,
                -(1.0 + fees),
                self.beta_val * (1.0 - fees),
            ),
            PositionKind::LONG => (
                self.units_to_buy_long_spread,
                1.0 - fees,
                -(self.beta_val * (1.0 + fees)),
            ),
        };
        self.value_strat += spread * (pos.right_price * right_coef + pos.left_price * left_coef);
        self.log_position(&pos, &Operation::CLOSE, spread, right_coef, left_coef);
    }
}

pub struct Strategy {
    fees_rate: f64,
    res_threshold_long: f64,
    res_threshold_short: f64,
    stop_loss: f64,
    evaluations_since_last: i32,
    beta_eval_window_size: i32,
    beta_eval_freq: i32,
    state: MovingState,
    data_table: DataTable,
    pub right_pair: &'static str,
    pub left_pair: &'static str,
    #[allow(dead_code)]
    metrics: Arc<StrategyMetrics>,
    db: Db,
}

impl Strategy {
    fn new(left_pair: &'static str, right_pair: &'static str) -> Self {
        let db_name = format!("{}_{}", left_pair, right_pair);
        Self {
            fees_rate: 0.001,
            res_threshold_long: -0.04,
            res_threshold_short: 0.04,
            stop_loss: -0.1,
            evaluations_since_last: 0,
            beta_eval_window_size: 500,
            beta_eval_freq: 5000,
            state: MovingState::new(100.0),
            data_table: DataTable::new(500),
            right_pair,
            left_pair,
            metrics: Arc::new(StrategyMetrics::for_strat(
                prometheus::default_registry(),
                left_pair,
                right_pair,
            )),
            db: Db::new("data/naive_pair_trading", db_name),
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
            println!(
                "eval_time : {} % {} == 0",
                self.evaluations_since_last, self.beta_eval_freq
            );
        }
        x
    }

    fn update_model(&mut self) {
        let beta = self.data_table.beta();
        self.state.beta_val = beta;
        self.state.alpha_val = self.data_table.alpha(beta);
    }

    fn predict(&self, bp: &BookPosition) -> f64 {
        self.data_table
            .predict(self.state.alpha_val, self.state.beta_val, bp)
    }

    fn set_long_spread(&mut self, traded_price: f64) {
        self.state.units_to_buy_long_spread =
            self.state.value_strat / (traded_price * (1.0 + self.fees_rate));
    }

    fn set_short_spread(&mut self, traded_price: f64) {
        self.state.units_to_buy_short_spread =
            self.state.value_strat / (traded_price * self.state.beta_val * (1.0 + self.fees_rate));
    }

    fn short_position(&self, lr: &DataRow) -> Position {
        Position {
            kind: PositionKind::SHORT,
            right_price: lr.right.bid,
            left_price: lr.left.ask,
            time: lr.time,
            right_pair: self.right_pair.to_string(),
            left_pair: self.left_pair.to_string(),
        }
    }

    fn long_position(&self, lr: &DataRow) -> Position {
        Position {
            kind: PositionKind::LONG,
            right_price: lr.right.ask,
            left_price: lr.left.bid,
            time: lr.time,
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
        self.state.predicted_right = self.predict(&lr.left);

        if self.state.beta_lr >= 0.0 {
            self.state.res = (lr.right.mid - self.state.predicted_right) / lr.right.mid;
        } else {
            self.state.res = 0.0;
        }
        if (self.state.res > self.res_threshold_short) && self.state.no_position_taken() {
            self.short_position(lr);
            self.state.open(self.short_position(lr), self.fees_rate);
        }

        if self.state.is_short() {
            let short_position_return =
                self.state
                    .set_short_position_return(self.fees_rate, lr.right.ask, lr.left.bid);
            if (self.state.res <= self.res_threshold_short && self.state.res < 0.0)
                || short_position_return < self.stop_loss
            {
                if short_position_return < self.stop_loss {
                    self.log_stop_loss(PositionKind::SHORT);
                }
                self.state.close(self.short_position(lr), self.fees_rate);
                self.state.set_pnl();
                self.state.beta_val = self.data_table.beta();
                self.state.alpha_val = self.data_table.alpha(self.state.beta_val);
            }
        }

        if self.state.res <= self.res_threshold_long && self.state.no_position_taken() {
            self.state.open(self.long_position(lr), self.fees_rate);
        }

        if self.state.is_long() {
            let long_position_return =
                self.state
                    .set_long_position_return(self.fees_rate, lr.right.bid, lr.left.ask);
            if (self.state.res >= self.res_threshold_long && self.state.res > 0.0)
                || long_position_return < self.stop_loss
            {
                if long_position_return < self.stop_loss {
                    self.log_stop_loss(PositionKind::LONG);
                }
                self.state.close(self.long_position(lr), self.fees_rate);
                self.state.pnl = self.state.value_strat;
                self.state.beta_val = self.data_table.beta();
                self.state.alpha_val = self.data_table.alpha(self.state.beta_val);
            }
        }
    }

    fn process_row(&mut self, row: &DataRow) {
        self.inc_evaluations_since_last();
        if self.data_table.rows.len() > self.beta_eval_window_size as usize {
            self.eval_latest(row);
        }
        self.data_table.push(row);
        if self.data_table.rows.len() == self.beta_eval_window_size as usize {
            self.update_model();
            self.set_long_spread(row.right.ask);
            self.set_short_spread(row.left.ask);
            self.state.set_pnl();
        }
    }
}

impl StrategySink for Strategy {
    fn add_event(&self, _: LiveEvent) -> std::io::Result<()> {
        unimplemented!()
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
}

impl DataTable {
    fn new(window_size: usize) -> Self {
        DataTable {
            rows: Vec::new(),
            window_size,
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
    use chrono::{DateTime, TimeZone, Utc};
    use itertools::Itertools;
    use plotters::prelude::*;
    use serde::{Deserialize, Serialize};
    use std::io::Result;

    use crate::serdes::date_time_format;
    use crate::strategies::naive_pair_trading::{
        BookPosition, DataRow, DataTable, MovingState, Strategy,
    };
    use crate::util::date::{DateRange, DurationRangeType};
    use ordered_float::OrderedFloat;
    use std::error::Error;
    use std::fs::File;
    use std::path::Path;
    use std::time::Instant;

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

    #[derive(Debug, Serialize, Deserialize)]
    struct CsvRecord {
        #[serde(with = "date_time_format")]
        hourofday: DateTime<Utc>,
        a1: f64,
        aq1: f64,
        a2: f64,
        aq2: f64,
        a3: f64,
        aq3: f64,
        a4: f64,
        aq4: f64,
        a5: f64,
        aq5: f64,
        b1: f64,
        bq1: f64,
        b2: f64,
        bq2: f64,
        b3: f64,
        bq3: f64,
        b4: f64,
        bq4: f64,
        b5: f64,
        bq5: f64,
    }

    fn read_csv(path: &str) -> Result<Vec<CsvRecord>> {
        let f = File::open(path)?;
        let mut rdr = csv::Reader::from_reader(f);
        let vec: Vec<CsvRecord> = rdr
            .deserialize()
            .map(|r| {
                let record: Result<CsvRecord> = r.map_err(|e| e.into());
                record.ok()
            })
            .while_some()
            .collect(); // just skip invalid rows
        Ok(vec)
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
            .unwrap_or("../data".to_string());
        let exchange_name = "Binance";
        let channel = "order_books";

        let base_path = Path::new(&bp)
            .join("data")
            .join(exchange_name)
            .join(channel);
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
            ("value", vec![|x| x.right_mid, |x| x.state.predicted_right]),
            (
                "return",
                vec![|x| x.state.short_position_return + x.state.long_position_return],
            ),
            ("PnL", vec![|x| x.state.pnl]),
            ("Nominal Position", vec![|x| x.state.nominal_position]),
            ("Beta", vec![|x| x.state.beta_lr]),
            ("res", vec![|x| x.state.res]),
            ("traded_price_left", vec![|x| x.state.traded_price_left]),
            ("traded_price_right", vec![|x| x.state.traded_price_right]),
            ("alpha_val", vec![|x| x.state.alpha_val]),
            ("value_strat", vec![|x| x.state.value_strat]),
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

    #[test]
    fn beta_val() {
        let mut dt = DataTable {
            rows: Vec::new(),
            window_size: 500,
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
        env_logger::init();
        let mut strat = Strategy::new(LEFT_PAIR, RIGHT_PAIR);
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
                    println!(
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
        // let logs_f = std::fs::File::create("strategy_logs.json").unwrap();
        // serde_json::to_writer(logs_f, &logs);
        let drew = draw_line_plot(logs);
        if let Ok(file) = drew {
            let copied = std::fs::copy(&file, "graphs/naive_pair_trading_plot_latest.svg");
            assert!(copied.is_ok(), format!("{:?}", copied));
        } else {
            assert!(false, format!("{:?}", drew));
        }
    }
}
