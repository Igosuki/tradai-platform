use average::Mean;
use itertools::Itertools;
use stats::stddev;

use crate::math::iter::{CovarianceExt, MeanExt, VarianceExt};

struct MovingState {
    position_short: i8,
    position_long: i8,
    value_strat: f64,
    units_to_buy_long_spread: f64,
    units_to_buy_short_spread: f64,
    evaluations_since_last: i32,
    beta_val: f64,
    preditected_right: f64,
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
            preditected_right: 0.0,
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
                + current_price_left * self.beta_val * (1.0 + fees_rate))
            - current_price_right * (1.0 - fees_rate);
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
}

impl Strategy {
    fn new() -> Strategy {
        Strategy {
            initial_value_strat: 100,
            fees_rate: 0.001,
            res_threshold_long: -0.0,
            res_threshold_short: 0.04,
            stop_loss: -0.2,
            beta_eval_window_size: 500,
            beta_eval_freq: 5000,
            state: MovingState::new(),
            data_table: DataTable::new(500),
        }
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
                self.state.value_strat / lr.right.top_ask * (1.0 + self.fees_rate);
            self.state.units_to_buy_short_spread = self.state.value_strat / lr.left.top_ask
                * self.state.beta_val
                * (1.0 + self.fees_rate);
        }
        self.state.preditected_right = self.data_table.predict(&lr.left);
        if self.state.beta_val >= 0.0 {
            self.state.res = (&lr.right.mid - self.state.preditected_right) / &lr.right.mid;
        } else {
            self.state.res = 0.0;
        }
        if (self.state.res > self.res_threshold_short)
            && (self.state.position_short == 0)
            && (self.state.position_long == 0)
        {
            self.state.set_position_short();
            self.state.open_position = 1e5;
            self.state.nominal_position = self.state.beta_val;
            self.state.traded_price_right = lr.right.top_bid;
            self.state.traded_price_left = lr.left.top_ask;
            self.state.value_strat += self.state.units_to_buy_short_spread
                * (self.state.traded_price_right * (1.0 - self.fees_rate)
                    - self.state.traded_price_left * self.state.beta_val * (1.0 + self.fees_rate));
            debug!("Open short position at {}", lr.time);
            // debug!("sell {} {} at {} for {} ", round(units_to_buy_short_spread, 2), crypto_pair[2], round(pair_data[i,'crypto2_b', with = F], 2), 'for', round(units_to_buy_short_spread*pair_data[i,'crypto2_b', with = F] * (1 - fees_rate), 2)  ))
            // debug!("buy {}", paste('buy', round(beta_val*units_to_buy_short_spread,2), crypto_pair[1], 'at', round(pair_data[i,'crypto1_a', with = F], 2), 'for',  round(beta_val*units_to_buy_short_spread*pair_data[i,'crypto1_a', with = F] * (1 + fees_rate), 2) ))
            debug!("--------------------------------")
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
                debug!("---- Stop-loss executed (short position) ----")
            }
            self.state.unset_position_short();
            self.state.close_position = 1e5;
            self.state.value_strat += self.state.units_to_buy_short_spread
                * lr.left.top_bid
                * self.state.beta_val
                * (1.0 - self.fees_rate)
                - lr.right.top_ask * (1.0 + self.fees_rate);
            debug!("Close short position at {}", lr.time);
            // debug!("buy {} {} at {} for {}", round(units_to_buy_short_spread, 2), crypto_pair[2], round(pair_data[i,'crypto2_a', with = F], 2), round(units_to_buy_short_spread*pair_data[i,'crypto2_a', with = F] * (1 + fees_rate), 2)))
            // debug!("sell {} {} at {} for {}", round(beta_val*units_to_buy_short_spread, 2), crypto_pair[1], 'at', round(pair_data[i,'crypto1_b', with = F], 2), 'for', round(beta_val*units_to_buy_short_spread*pair_data[i,'crypto1_b', with = F] * ( 1 - fees_rate), 2)))
            debug!("strategy value : {}", self.state.value_strat);
            self.state.pnl = self.state.value_strat;
            debug!("--------------------------------");
            debug!("--------------------------------");
            self.state.beta_val = self.data_table.beta();
        }
        if self.state.res <= self.res_threshold_long && self.state.no_short_no_long() {
            self.state.set_position_long();
            self.state.open_position = 1e5;
            self.state.nominal_position = self.state.beta_val;
            self.state.traded_price_right = lr.right.top_ask;
            self.state.traded_price_left = lr.left.top_bid;
            self.state.value_strat += self.state.units_to_buy_long_spread
                * self.state.traded_price_left
                * self.state.beta_val
                * (1.0 - self.fees_rate)
                - self.state.traded_price_right * (1.0 + self.fees_rate);
            debug!("Open long position at {} ", lr.time);
            // debug!("buy {} {} at {} for {}" , round(units_to_buy_long_spread, 2), crypto_pair[2], 'at', round(pair_data[i,'crypto2_a', with = F], 2), 'for', round(units_to_buy_long_spread*pair_data[i,'crypto2_a', with = F] * (1 + fees_rate), 2)  ))
            // debug!("sell {} {} at {} for {}" , round(units_to_buy_long_spread*beta_val,2), crypto_pair[1], 'at', round(pair_data[i,'crypto1_b', with = F], 2), 'for', round(beta_val*units_to_buy_long_spread*pair_data[i,'crypto1_b', with = F] * ( 1 - fees_rate), 2)))
            debug!("--------------------------------")
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
                    debug!("---- Stop-loss executed (long position) ----")
                }
                self.state.unset_position_long();
                self.state.close_position = 1e5;
                self.state.value_strat +=
                    self.state.units_to_buy_long_spread * lr.right.top_bid * (1.0 - self.fees_rate)
                        - lr.left.top_ask * self.state.beta_val * (1.0 + self.fees_rate);
                debug!("Close long position at {}", lr.time);
                // debug!("sell {} {} at {} for {}", round(units_to_buy_long_spread, 2), crypto_pair[2], 'at', round(pair_data[i,'crypto2_b', with = F], 2), 'for', round(units_to_buy_long_spread*pair_data[i,'crypto2_b', with = F] * (1 - fees_rate), 2)  ))
                // debug!("buy {} {} at {} for {}", round(units_to_buy_long_spread*beta_val,2), crypto_pair[1], 'at', round(pair_data[i,'crypto1_a', with = F], 2), 'for', round(units_to_buy_long_spread*beta_val*pair_data[i,'crypto1_a', with = F] * (1 + fees_rate), 2)  ))
                debug!("strategy value : {}", self.state.value_strat);
                self.state.pnl = self.state.value_strat;
                debug!("--------------------------------");
                debug!("--------------------------------");
                self.state.beta_val = self.data_table.beta();
            }
        }
    }

    fn process_row(&mut self, row: &DataRow) {
        self.eval_latest(row);
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
    time: i64,
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
            &self.rows[(len - self.window_size)..(len - 1)]
        }
    }

    pub fn push(&mut self, row: &DataRow) {
        self.rows.push(row.clone());
    }

    pub fn beta(&self) -> f64 {
        let (for_variance, for_covariance) = self.current_window().iter().tee();
        let mut left = for_variance.map(|r| r.left.mid);
        let variance: f64 = left.variance();
        debug!("variance {:?}", variance);
        let covariance: f64 = for_covariance
            .map(|r| (r.left.mid, r.right.mid))
            // originally if left and mid may not be present
            // .take_while(|p| p.0.is_some() && p.1.is_some())
            // .map(|p| (p.0.unwrap(), p.1.unwrap()))
            .covariance::<(f64, f64), f64>();
        debug!("covariance {:?}", covariance);
        let beta_val = covariance / variance;
        debug!("beta_val {:?}", beta_val);
        beta_val
    }

    pub fn alpha(&self, beta_val: f64) -> f64 {
        let (iter_left, iter_right) = self.current_window().iter().tee();
        let mean_left: f64 = iter_left.map(|l| l.left.mid).mean();
        debug!("mean left {:?}", mean_left);
        let mean_right: f64 = iter_right.map(|l| l.right.mid).mean();
        debug!("mean right {:?}", mean_right);
        mean_right - beta_val * mean_left
    }

    fn predict(&self, bp: &BookPosition) -> f64 {
        // predict(model_value, pair_data[i,"crypto1_m"]) ->
        // alpha_val + beta_val * pair_data[i,"crypto1_m"])
        // alpha_val = mean(crypto2_m) - beta_val * mean(crypto1_m)
        let beta_val: f64 = self.beta();
        let alpha_val: f64 = self.alpha(beta_val);
        alpha_val + beta_val * bp.mid
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
    use crate::util::date::DateRange;
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
    }

    impl StrategyLog {
        fn from_state(time: DateTime<Utc>, state: &MovingState, last_row: &DataRow) -> StrategyLog {
            StrategyLog {
                time,
                right_mid: last_row.right.mid,
                left_mid: last_row.left.mid,
                predicted_right_price: state.preditected_right,
                long_position_return: state.long_position_return,
                short_position_return: state.short_position_return,
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

    fn draw_line_plot(data: Vec<StrategyLog>) -> std::result::Result<(), Box<dyn Error>> {
        let now = Utc::now();
        let string = format!("naive_pair_trading_plot_{}.png", now);
        let root = BitMapBackend::new(&string, (1024, 768)).into_drawing_area();
        root.fill(&WHITE)?;

        // plot(x_time_posixlt, crypto_right_mid, type = 's', xaxt = "n", xlab = "time", ylab = "value",
        // lim = c(min(pair_data[(beta_eval_window_size+1): nrow(pair_data),"predicted_crypto2"]), max(pair_data$predicted_crypto2)))
        // lines(x_time_posixlt, pair_data$predicted_crypto2[beta_eval_window_size+1: nrow(pair_data)], type = 's', xaxt = "n", xlab = "time", col= "blue")
        // axis.POSIXct(1, x_time_posixlt, format="%m-%d %H:%M", at = x_time_posixlt, las=2, cex.axis = .5, srt = 45, xpd=TRUE)

        let lower = data.first().unwrap().time.date();
        let upper = data.last().unwrap().time.date();
        let x_range = lower..upper;

        let (mins, maxs) = data
            .iter()
            .skip(500)
            .map(|sl| OrderedFloat(sl.predicted_right_price))
            .tee();
        let y_range = mins.min().unwrap().0..maxs.max().unwrap().0;

        let mut chart = ChartBuilder::on(&root)
            .x_label_area_size(40)
            .y_label_area_size(40)
            .caption("value", ("sans-serif", 50.0).into_font())
            .build_ranged(x_range, y_range)?;

        chart.configure_mesh().line_style_2(&WHITE).draw()?;

        chart.draw_series(LineSeries::new(
            data.iter().map(|x| (x.time.date(), x.right_mid)),
            &BLACK,
        ))?;
        chart.draw_series(LineSeries::new(
            data.iter()
                .map(|x| (x.time.date(), x.predicted_right_price)),
            &BLUE,
        ))?;

        Ok(())
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
        let (left_records, right_records) = load_csv_dataset(&DateRange(dt0, dt1));
        // align data
        left_records
            .into_iter()
            .zip(right_records.into_iter())
            .take(500)
            .for_each(|(l, r)| {
                dt.push(&DataRow {
                    time: l.time(),
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
        let mut strat = Strategy::new();
        // Read downsampled streams
        let dt0 = Utc.ymd(2020, 03, 25);
        let dt1 = Utc.ymd(2020, 04, 08);
        let (left_records, right_records) = load_csv_dataset(&DateRange(dt0, dt1));
        println!("Dataset loaded in memory...");
        // align data
        let logs = left_records
            .into_iter()
            .zip(right_records.into_iter())
            .enumerate()
            .map(|(i, (l, r))| {
                if (i % 1000 == 0) {
                    println!("{} iterations...", i);
                }
                let row_time = l.time();
                let row = DataRow {
                    time: row_time,
                    left: to_pos(l),
                    right: to_pos(r),
                };
                strat.process_row(&row);
                StrategyLog::from_state(Utc.timestamp(row_time, 0), &strat.state, &row)
            })
            .collect();
        draw_line_plot(logs);
        assert!(true, false);
    }
}

// par(mfrow=c(3,1))
// plot(x_time_posixlt, crypto_right_mid, type = 's', xaxt = "n", xlab = "time", ylab = "value", lim = c(min(pair_data[(beta_eval_window_size+1): nrow(pair_data),"predicted_crypto2"]), max(pair_data$predicted_crypto2)))
// lines(x_time_posixlt, pair_data$predicted_crypto2[beta_eval_window_size+1: nrow(pair_data)], type = 's', xaxt = "n", xlab = "time", col= "blue")
// axis.POSIXct(1, x_time_posixlt, format="%m-%d %H:%M", at = x_time_posixlt, las=2, cex.axis = .5, srt = 45, xpd=TRUE)
// plot(x_time_posixlt, y_(long_position_return + short_position_return), type = 's', xaxt = "n", xlab = "time", ylab = "Position return")
// plot(x_time_posixlt, y_res_column, type = 's', xaxt = "n", xlab = "time", col= "red")

// dev.new()
// par(mfrow=c(3,1))
// par(mar = c(10,4,4,2) + 0.1)
// plot(as.POSIXlt(pnl_ts$time), pnl_ts$PnL, type = 's', xaxt = "n", xlab = "time", ylab = "PnL")
// axis.POSIXct(1, as.POSIXlt(pnl_ts$time), format="%m-%d %H:%M", at = as.POSIXlt(pnl_ts$time), las=2, cex.axis = .5, srt = 45, xpd=TRUE)
// plot(as.POSIXlt(pair_data[nominal_position>0, time]), pair_data[nominal_position>0, nominal_position], type = 's', xaxt = "n", xlab = "time", ylab = "Nominal position")
// axis.POSIXct(1, as.POSIXlt(pnl_ts$time), format="%m-%d %H:%M", at = as.POSIXlt(pnl_ts$time), las=2, cex.axis = .5, srt = 45, xpd=TRUE)
// plot(x_time_posixlt, pair_data$beta_lr[beta_eval_window_size+1: nrow(pair_data)], type = 's', xaxt = "n", xlab = "time", ylab = "Beta")
