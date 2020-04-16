use average::Mean;
use itertools::Itertools;
use stats::stddev;

use crate::math::iter::{CovarianceExt, MeanExt, VarianceExt};
use chrono::{DateTime, Utc};
use serde::export::Formatter;
use std::fmt::Display;

const TS_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

enum Position {
    SHORT,
    LONG,
}

enum Operation {
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
        });
        Ok(())
    }
}

struct MovingState {
    position_short: i8,
    position_long: i8,
    value_strat: f64,
    units_to_buy_long_spread: f64,
    units_to_buy_short_spread: f64,
    evaluations_since_last: i32,
    beta_val: f64,
    beta_lr: f64,
    predicted_right: f64,
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
    fn new() -> MovingState {
        MovingState {
            position_short: 0,
            position_long: 0,
            value_strat: 100.0,
            units_to_buy_long_spread: 0.0,
            units_to_buy_short_spread: 0.0,
            evaluations_since_last: 500,
            beta_val: 0.0,
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

    fn log_pos(&self, op: Operation, pos: &Position, time: DateTime<Utc>) {
        debug!(
            "{} {} position at {}",
            op,
            match pos {
                Position::SHORT => "short",
                Position::LONG => "long",
            },
            time.format(TS_FORMAT)
        );
    }

    fn log_trade(&self, op: Operation, spread: f64, pair: &str, value: f64, qty: f64) {
        debug!("{} {:.2} {} at {} for {:.2}", op, spread, pair, value, qty);
    }

    fn log_info(&self, pos: &Position) {
        debug!(
            "Additional info : units {:.2} beta val {:.2} value strat {:.2}",
            match pos {
                Position::SHORT => self.units_to_buy_short_spread,
                Position::LONG => self.units_to_buy_long_spread,
            },
            self.beta_val,
            self.value_strat
        );
        debug!("--------------------------------")
    }

    fn no_short_no_long(&self) -> bool {
        self.position_short == 0 && self.position_long == 0
    }

    fn is_long(&self) -> bool {
        self.position_long == 1
    }

    fn is_short(&self) -> bool {
        self.position_short == 1
    }

    fn inc_evaluations_since_last(&mut self) {
        self.evaluations_since_last += 1;
    }

    fn set_position_short(&mut self) {
        self.position_short = 1;
    }

    fn unset_position_short(&mut self) {
        self.position_short = 0;
    }

    fn set_position_long(&mut self) {
        self.position_long = 1;
    }

    fn unset_position_long(&mut self) {
        self.position_long = 0;
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
            / self.value_strat;
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
            / self.value_strat;
        self.short_position_return = x;
        return x;
    }

    fn open_position(
        &mut self,
        pos: Position,
        right_price: f64,
        left_price: f64,
        fees: f64,
        time: DateTime<Utc>,
        right_pair: &str,
        left_pair: &str,
    ) {
        match pos {
            Position::SHORT => self.set_position_short(),
            Position::LONG => self.set_position_long(),
        };
        self.open_position = 1e5;
        self.nominal_position = self.beta_val;
        self.traded_price_right = right_price;
        self.traded_price_left = left_price;
        let (spread, right_coef, left_coef) = match pos {
            Position::SHORT => (
                self.units_to_buy_short_spread,
                1.0 - fees,
                -(self.beta_val * (1.0 + fees)),
            ),
            Position::LONG => (
                self.units_to_buy_long_spread,
                -(1.0 + fees),
                self.beta_val * (1.0 - fees),
            ),
        };
        self.value_strat += spread * (right_price * right_coef + left_price * left_coef);
        self.log_pos(Operation::OPEN, &pos, time);
        let (left_op, right_op) = match &pos {
            Position::SHORT => (Operation::BUY, Operation::SELL),
            Position::LONG => (Operation::SELL, Operation::BUY),
        };
        self.log_trade(
            right_op,
            spread,
            right_pair,
            right_price,
            spread * right_price * right_coef.abs(),
        );
        self.log_trade(
            left_op,
            spread * self.beta_val,
            left_pair,
            left_price,
            spread * left_price * left_coef.abs(),
        );
        self.log_info(&pos);
    }

    fn close_position(
        &mut self,
        pos: Position,
        right_price: f64,
        left_price: f64,
        fees: f64,
        time: DateTime<Utc>,
        right_pair: &str,
        left_pair: &str,
    ) {
        match pos {
            Position::SHORT => self.unset_position_short(),
            Position::LONG => self.unset_position_long(),
        };
        self.close_position = 1e5;
        let (spread, right_coef, left_coef) = match pos {
            Position::SHORT => (
                self.units_to_buy_short_spread,
                -(1.0 + fees),
                self.beta_val * (1.0 - fees),
            ),
            Position::LONG => (
                self.units_to_buy_long_spread,
                1.0 - fees,
                -(self.beta_val * (1.0 + fees)),
            ),
        };
        self.value_strat += spread * (right_price * right_coef + left_price * left_coef);
        self.log_pos(Operation::CLOSE, &pos, time);
        let (left_op, right_op) = match pos {
            Position::SHORT => (Operation::SELL, Operation::BUY),
            Position::LONG => (Operation::BUY, Operation::SELL),
        };
        self.log_trade(
            right_op,
            spread,
            right_pair,
            right_price,
            spread * right_price * right_coef.abs(),
        );
        self.log_trade(
            left_op,
            spread * self.beta_val,
            left_pair,
            left_price,
            spread * left_price * left_coef.abs(),
        );
        self.log_info(&pos);
    }
}

pub struct Strategy {
    initial_value_strat: i32,
    fees_rate: f64,
    res_threshold_long: f64,
    res_threshold_short: f64,
    stop_loss: f64,
    beta_eval_window_size: i32,
    beta_eval_freq: i32,
    state: MovingState,
    data_table: DataTable,
    pub right_pair: &'static str,
    pub left_pair: &'static str,
}

impl Strategy {
    fn new() -> Strategy {
        Strategy {
            initial_value_strat: 100,
            fees_rate: 0.001,
            res_threshold_long: -0.04,
            res_threshold_short: 0.04,
            stop_loss: -0.2,
            beta_eval_window_size: 500,
            beta_eval_freq: 5000,
            state: MovingState::new(),
            data_table: DataTable::new(500),
            right_pair: "BTC_USDT",
            left_pair: "ETH_USDT",
        }
    }

    fn log_stop_loss(&self, pos: Position) {
        debug!(
            "---- Stop-loss executed ({} position) ----",
            match pos {
                Position::SHORT => "short",
                Position::LONG => "long",
            }
        )
    }

    fn should_eval(&self) -> bool {
        self.state.evaluations_since_last % self.beta_eval_freq == 0
    }

    fn eval_latest(&mut self, lr: &DataRow) {
        self.state.inc_evaluations_since_last();
        if self.state.no_short_no_long() {
            if self.should_eval() {
                self.state.beta_val = self.data_table.beta();
            }
            self.state.units_to_buy_long_spread =
                self.state.value_strat / (lr.right.top_ask * (1.0 + self.fees_rate));
            self.state.units_to_buy_short_spread = self.state.value_strat
                / (lr.left.top_ask * self.state.beta_val * (1.0 + self.fees_rate));
        }

        self.state.beta_lr = self.state.beta_val;

        self.state.predicted_right = self.data_table.predict(self.state.beta_val, &lr.left);
        if self.state.beta_lr >= 0.0 {
            self.state.res = (lr.right.mid - self.state.predicted_right) / lr.right.mid;
        } else {
            self.state.res = 0.0;
        }

        if (self.state.res > self.res_threshold_short) && self.state.no_short_no_long() {
            self.state.open_position(
                Position::SHORT,
                lr.right.top_bid,
                lr.left.top_ask,
                self.fees_rate,
                lr.time,
                self.right_pair,
                self.left_pair,
            );
        }

        if self.state.is_short() {
            let short_position_return = self.state.set_short_position_return(
                self.fees_rate,
                lr.right.top_ask,
                lr.left.top_bid,
            );

            if (self.state.res <= self.res_threshold_short && self.state.res < 0.0)
                || short_position_return < self.stop_loss
            {
                if short_position_return < self.stop_loss {
                    self.log_stop_loss(Position::SHORT);
                }
                self.state.close_position(
                    Position::SHORT,
                    lr.right.top_ask,
                    lr.left.top_bid,
                    self.fees_rate,
                    lr.time,
                    self.right_pair,
                    self.left_pair,
                );
                self.state.pnl = self.state.value_strat;
                self.state.beta_val = self.data_table.beta();
            }
        }

        if self.state.res <= self.res_threshold_long && self.state.no_short_no_long() {
            self.state.open_position(
                Position::LONG,
                lr.right.top_ask,
                lr.left.top_bid,
                self.fees_rate,
                lr.time,
                self.right_pair,
                self.left_pair,
            );
        }

        if self.state.is_long() {
            let long_position_return = self.state.set_long_position_return(
                self.fees_rate,
                lr.right.top_bid,
                lr.left.top_ask,
            );
            if (self.state.res >= self.res_threshold_long && self.state.res > 0.0)
                || long_position_return < self.stop_loss
            {
                if long_position_return < self.stop_loss {
                    self.log_stop_loss(Position::LONG);
                }
                self.state.close_position(
                    Position::LONG,
                    lr.right.top_bid,
                    lr.left.top_ask,
                    self.fees_rate,
                    lr.time,
                    self.right_pair,
                    self.left_pair,
                );
                self.state.pnl = self.state.value_strat;
                self.state.beta_val = self.data_table.beta();
            }
        }
    }

    fn process_row(&mut self, row: &DataRow) {
        if self.data_table.rows.len() > self.beta_eval_window_size as usize {
            self.eval_latest(row);
        } else if self.data_table.rows.len() == self.beta_eval_window_size as usize {
            self.state.beta_val = self.data_table.beta();
            self.state.units_to_buy_long_spread =
                self.state.value_strat / (row.right.top_ask * (1.0 + self.fees_rate));
            self.state.units_to_buy_short_spread = self.state.value_strat
                / (row.left.top_ask * self.state.beta_val * (1.0 + self.fees_rate));
        }
        self.data_table.push(row);
    }
}

#[derive(Debug, Clone)]
struct BookPosition {
    pub mid: f64,
    // mid = (top_ask + top_bid) / 2, alias: crypto1_m
    top_ask: f64,
    // crypto_a
    top_ask_q: f64,
    // crypto_a_q
    top_bid: f64,
    // crypto_b
    top_bid_q: f64, // crypto_b_q
                    // log_r: f64, // crypto_log_r
                    // log_r_10: f64, // crypto_log_r
}

impl BookPosition {
    fn mid(&self) -> f64 {
        (self.top_ask + self.top_bid) / 2.0
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

    fn current_window(&self) -> &[DataRow] {
        let len = self.rows.len();
        if len <= self.window_size {
            &self.rows[..]
        } else {
            &self.rows[(len - self.window_size - 1)..]
        }
    }

    pub fn push(&mut self, row: &DataRow) {
        self.rows.push(row.clone());
    }

    pub fn beta(&self) -> f64 {
        let (for_variance, for_covariance) = self.current_window().iter().tee();
        let mut left = for_variance.map(|r| r.left.mid);
        let variance: f64 = left.variance();
        trace!("variance {:?}", variance);
        let covariance: f64 = for_covariance
            .map(|r| (r.left.mid, r.right.mid))
            // originally if left and mid may not be present
            // .take_while(|p| p.0.is_some() && p.1.is_some())
            // .map(|p| (p.0.unwrap(), p.1.unwrap()))
            .covariance::<(f64, f64), f64>();
        trace!("covariance {:?}", covariance);
        let beta_val = covariance / variance;
        trace!("beta_val {:?}", beta_val);
        beta_val
    }

    pub fn alpha(&self, beta_val: f64) -> f64 {
        let (iter_left, iter_right) = self.current_window().iter().tee();
        let mean_left: f64 = iter_left.map(|l| l.left.mid).mean();
        trace!("mean left {:?}", mean_left);
        let mean_right: f64 = iter_right.map(|l| l.right.mid).mean();
        trace!("mean right {:?}", mean_right);
        mean_right - beta_val * mean_left
    }

    fn predict(&self, beta_val: f64, bp: &BookPosition) -> f64 {
        let alpha_val: f64 = self.alpha(beta_val);
        let p = alpha_val + beta_val * bp.mid;
        trace!("predict {:?}", p);
        p
    }

    fn last_row(&self) -> Option<&DataRow> {
        self.rows.last()
    }
}

#[cfg(test)]
mod test {
    use chrono::serde::ts_seconds;
    use chrono::{Date, DateTime, Duration, TimeZone, Utc};
    use itertools::Itertools;
    use plotters::prelude::*;
    use serde::{Deserialize, Serialize};
    use stats::stddev;
    use std::borrow::BorrowMut;
    use std::cell::RefCell;
    use std::fs::File;
    use std::io::Result;
    use std::rc::Rc;

    use crate::serdes::date_time_format;
    use crate::strategies::naive_pair_trading::{
        BookPosition, DataRow, DataTable, MovingState, Strategy,
    };
    use crate::util::date::{DateRange, DurationRangeType};
    use ordered_float::OrderedFloat;
    use plotters::drawing::BitMapBackend;
    use std::error::Error;
    use std::ops::Range;
    use std::path::Path;

    #[derive(Debug, Serialize, Deserialize)]
    struct StrategyLog {
        time: DateTime<Utc>,
        right_mid: f64,
        left_mid: f64,
        predicted_right_price: f64,
        long_position_return: f64,
        short_position_return: f64,
        res: f64,
        nominal_pos: f64,
        beta: f64,
        pnl: f64,
        value_strat: f64,
    }

    impl StrategyLog {
        fn from_state(time: DateTime<Utc>, state: &MovingState, last_row: &DataRow) -> StrategyLog {
            StrategyLog {
                time,
                right_mid: last_row.right.mid,
                left_mid: last_row.left.mid,
                predicted_right_price: state.predicted_right,
                long_position_return: state.long_position_return,
                short_position_return: state.short_position_return,
                res: state.res,
                nominal_pos: state.nominal_position,
                beta: state.beta_lr,
                pnl: state.pnl,
                value_strat: state.value_strat,
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

    impl CsvRecord {
        fn time(&self) -> i64 {
            self.hourofday.timestamp()
        }
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

    fn to_pos(r: CsvRecord) -> BookPosition {
        BookPosition {
            top_ask: r.a1,
            top_ask_q: r.aq1,
            top_bid: r.b1,
            top_bid_q: r.bq1,
            mid: (r.a1 + r.b1) / 2.0,
        }
    }

    fn load_records(path: &str) -> Vec<CsvRecord> {
        let dt1 = read_csv(path).unwrap();
        let dt1_iter: Vec<CsvRecord> = dt1
            .into_iter()
            .group_by(|r| r.hourofday.timestamp())
            .into_iter()
            .map(|(k, mut v)| v.next().unwrap())
            .collect();
        dt1_iter
    }

    fn load_csv_dataset(dr: &DateRange) -> (Vec<CsvRecord>, Vec<CsvRecord>) {
        let bp = std::env::var_os("BITCOINS_REPO")
            .and_then(|oss| oss.into_string().ok())
            .unwrap_or("../data".to_string());
        let exchange_name = "Binance";
        let channel = "order_books";
        let left_pair = "ETH_USDT";
        let right_pair = "BTC_USDT";

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
            get_records(left_pair.to_string()),
            get_records(right_pair.to_string()),
        );
    }

    fn draw_line_plot(data: Vec<StrategyLog>) -> std::result::Result<String, Box<dyn Error>> {
        let now = Utc::now();
        let string = format!(
            "naive_pair_trading_plot_{}.svg",
            now.format("%Y%m%d%H:%M:%S")
        );
        let root = SVGBackend::new(&string, (1724, 2048)).into_drawing_area();
        root.fill(&WHITE)?;

        let lower = data.first().unwrap().time;
        let upper = data.last().unwrap().time;
        let x_range = lower..upper;

        let area_rows = root.split_evenly((6, 1));

        let skipped_data = data.iter().skip(500);

        {
            let (mins, maxs) = skipped_data
                .clone()
                .map(|sl| OrderedFloat(sl.predicted_right_price))
                .tee();
            let y_range = mins.filter(|p| p.0 > 0.0).min().unwrap().0..maxs.max().unwrap().0;

            let mut chart = ChartBuilder::on(&area_rows[0])
                .x_label_area_size(60)
                .y_label_area_size(60)
                .caption("value", ("sans-serif", 50.0).into_font())
                .build_ranged(x_range.clone(), y_range)?;
            chart.configure_mesh().line_style_2(&WHITE).draw()?;

            chart.draw_series(LineSeries::new(
                data.iter().map(|x| (x.time, x.right_mid)),
                &BLACK,
            ))?;
            chart.draw_series(LineSeries::new(
                skipped_data
                    .clone()
                    .map(|x| (x.time, x.predicted_right_price)),
                &BLUE,
            ))?;
        }
        {
            let y_range = -1.0..1.0;
            let mut chart = ChartBuilder::on(&area_rows[1])
                .x_label_area_size(60)
                .y_label_area_size(60)
                .caption("return", ("sans-serif", 50.0).into_font())
                .build_ranged(x_range.clone(), y_range)?;
            chart.configure_mesh().line_style_2(&WHITE).draw()?;
            chart.draw_series(LineSeries::new(
                skipped_data
                    .clone()
                    .map(|x| (x.time, x.short_position_return + x.long_position_return)),
                &BLACK,
            ))?;
        }
        {
            let y_range = -1.0..1.0;
            let mut chart = ChartBuilder::on(&area_rows[2])
                .x_label_area_size(60)
                .y_label_area_size(60)
                .caption("res", ("sans-serif", 50.0).into_font())
                .build_ranged(x_range.clone(), y_range)?;
            chart.configure_mesh().line_style_2(&WHITE).draw()?;
            chart.draw_series(LineSeries::new(
                skipped_data.clone().map(|x| (x.time, x.res)),
                &RED,
            ))?;
        }
        {
            let y_range = 0.0..200.0;
            let mut chart = ChartBuilder::on(&area_rows[3])
                .x_label_area_size(60)
                .y_label_area_size(60)
                .caption("PnL", ("sans-serif", 50.0).into_font())
                .build_ranged(x_range.clone(), y_range)?;
            chart.configure_mesh().line_style_2(&WHITE).draw()?;
            chart.draw_series(LineSeries::new(
                skipped_data.clone().map(|x| (x.time, x.pnl)),
                &BLACK,
            ))?;
        }
        {
            let y_range = 0.0..100.0;
            let mut chart = ChartBuilder::on(&area_rows[4])
                .x_label_area_size(60)
                .y_label_area_size(60)
                .caption("Nominal Position", ("sans-serif", 50.0).into_font())
                .build_ranged(x_range.clone(), y_range)?;
            chart.configure_mesh().line_style_2(&WHITE).draw()?;
            chart.draw_series(LineSeries::new(
                skipped_data
                    .clone()
                    .filter(|d| d.nominal_pos > 0.0)
                    .map(|x| (x.time, x.nominal_pos)),
                &BLACK,
            ))?;
        }
        {
            let y_range = 0.0..100.0;
            let mut chart = ChartBuilder::on(&area_rows[5])
                .x_label_area_size(60)
                .y_label_area_size(60)
                .caption("Beta", ("sans-serif", 50.0).into_font())
                .build_ranged(x_range.clone(), y_range)?;
            chart.configure_mesh().line_style_2(&WHITE).draw()?;
            chart.draw_series(LineSeries::new(
                skipped_data
                    .clone()
                    .filter(|d| d.nominal_pos > 0.0)
                    .map(|x| (x.time, x.beta)),
                &BLACK,
            ))?;
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
            .into_iter()
            .zip(right_records.into_iter())
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

    use plotters::coord::Shift;
    use std::time::Instant;

    #[test]
    fn continuous_scenario() {
        env_logger::init();
        let mut strat = Strategy::new();
        // Read downsampled streams
        let dt0 = Utc.ymd(2020, 03, 25);
        let dt1 = Utc.ymd(2020, 04, 08);
        let (left_records, right_records) =
            load_csv_dataset(&DateRange(dt0, dt1, DurationRangeType::Days, 1));
        println!("Dataset loaded in memory...");
        // align data
        let mut elapsed = 0 as u128;
        let mut iterations = 0 as u128;
        let logs: Vec<StrategyLog> = left_records
            .into_iter()
            .zip(right_records.into_iter())
            .enumerate()
            .map(|(i, (l, r))| {
                iterations += 1;
                let now = Instant::now();

                if (i % 1000 == 0) {
                    println!("{} iterations...", i);
                }
                let log = {
                    let row_time = l.hourofday;
                    let row = DataRow {
                        time: row_time,
                        left: to_pos(l),
                        right: to_pos(r),
                    };
                    strat.process_row(&row);
                    StrategyLog::from_state(row_time, &strat.state, &row)
                };
                elapsed += now.elapsed().as_nanos();
                log
            })
            .collect();
        println!("Each iteration took {} on avg", elapsed / iterations);
        let logs_f = std::fs::File::create("strategy_logs.json").unwrap();
        serde_json::to_writer(logs_f, &logs);
        draw_line_plot(logs);
        assert!(true, false);
    }

    #[test]
    fn plot_continuous_scenario() {
        let logs_f = std::fs::File::open("strategy_logs.json").unwrap();
        let logs: Vec<StrategyLog> = serde_json::from_reader(logs_f).unwrap();
        let drew = draw_line_plot(logs);
        if let Ok(file) = drew {
            let copied = std::fs::copy(&file, "naive_pair_trading_plot_latest.svg");
            assert!(copied.is_ok(), format!("{:?}", copied));
        } else {
            assert!(false, format!("{:?}", drew));
        }
    }
}
