use stats::stddev;
use average::Mean;
use itertools::Itertools;

use crate::math::iter::{VarianceExt, CovarianceExt};

struct MovingState {
    position_short: i8,
    position_long: i8,
    value_strat: i32,
    units_to_buy_long_spread: f64,
    units_to_buy_short_spread: f64,
}

impl MovingState {
    fn is_none(&self) -> bool {
        self.position_short == 0 && self.position_long == 0
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
    latest_state: MovingState,
    pub evals_so_far: i32,
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
            latest_state: MovingState {
                position_short: 0,
                position_long: 0,
                value_strat: 100,
                units_to_buy_long_spread: 0.0,
                units_to_buy_short_spread: 0.0,
            },
            evals_so_far: 501,
            data_table: DataTable::new()
        }
    }

    fn get_long_position_value(&self, traded_price_crypto2: f64, traded_price_crypto1: f64, current_price_crypto2: f64, current_price_crypto1: f64, beta_val: f64, units_to_buy: f64) -> f64 {
        return units_to_buy * (traded_price_crypto1 * beta_val * (1.0 - self.fees_rate) - traded_price_crypto2 * (1.0 + self.fees_rate) + current_price_crypto2 * (1.0 - self.fees_rate) - current_price_crypto1 * beta_val * (1.0 + self.fees_rate));
    }

    fn get_long_position_return(&self, traded_price_crypto2: f64, traded_price_crypto1: f64, current_price_crypto2: f64, current_price_crypto1: f64, beta_val: f64, units_to_buy: f64, value_strat: f64) -> f64 {
        return self.get_long_position_value(traded_price_crypto2, traded_price_crypto1, current_price_crypto2, current_price_crypto1, beta_val, units_to_buy) / value_strat;
    }

    fn get_short_position_value(&self, traded_price_crypto2: f64, traded_price_crypto1: f64, current_price_crypto2: f64, current_price_crypto1: f64, beta_val: f64, units_to_buy: f64) -> f64 {
        return units_to_buy * (traded_price_crypto2 * (1.0 - self.fees_rate) - traded_price_crypto1 * beta_val * (1.0 + self.fees_rate) + current_price_crypto1 * beta_val * (1.0 - self.fees_rate) - current_price_crypto2 * (1.0 + self.fees_rate));
    }

    fn get_short_position_return(&self, traded_price_crypto2: f64, traded_price_crypto1: f64, current_price_crypto2: f64, current_price_crypto1: f64, beta_val: f64, units_to_buy: f64, value_strat: f64) -> f64 {
        return self.get_short_position_value(traded_price_crypto2, traded_price_crypto1, current_price_crypto2, current_price_crypto1, beta_val, units_to_buy) / value_strat;
    }

    //
    // model_value = lm(crypto2_m ~ crypto1_m, data = pair_data[1:beta_eval_window_size]) # lm(crypto2_m ~ crypto1_m + 0
    // beta_val = as.numeric(model_value$coef[2])
    // units_to_buy_long_spread = value_strat / (pair_data[i,'crypto2_a', with = F] * (1 + fees_rate))
    // units_to_buy_short_spread = value_strat / (pair_data[i, 'crypto1_a'] * beta_val * (1 + fees_rate))

    fn should_eval(&self) -> bool {
        self.evals_so_far % self.beta_eval_freq == 0
    }

    fn process_row(&mut self, row: DataRow) {
        self.evals_so_far += 1;
        if self.latest_state.is_none() {
            if self.should_eval() {
                //"beta_val = covariance(crypto2_m, crypto1_m)/variance(crypto1_m)"
                // model_value = lm(crypto2_m ~ crypto1_m, data = pair_data[(i-beta_eval_window_size):(i-1)])
                // beta_val = as.numeric(model_value$coef[2])
            }
            // units_to_buy_long_spread = value_strat / (pair_data[i,'crypto2_a', with = F] * (1 + fees_rate))
            // units_to_buy_short_spread = value_strat / (pair_data[i, 'crypto1_a'] * beta_val * (1 + fees_rate))
        }
    }

    fn predict(&self) {
        // predict(model_value, pair_data[i,"crypto1_m"])
        // deviendra
        // alpha_val = mean(crypto2_m) - beta_val * mean(crypto1_m)
        // alpha_val + beta_val * pair_data[i,"crypto1_m"])
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
    pub left: Option<BookPosition>,
    // crypto_1
    pub right: Option<BookPosition>, // crypto_2
}

#[derive(Debug)]
struct DataTable {
    rows: Vec<DataRow>,
}

impl DataTable {
    fn new() -> Self {
        DataTable {
            rows: Vec::new()
        }
    }

    pub fn push(&mut self, row: DataRow) {
        self.rows.push(row);
    }

    pub fn beta_val(&self) -> f64 {
        let iter = self.rows.clone().into_iter();
        let mut some1 = iter.map(|r| r.left).while_some();
        let variance: f64 = some1.by_ref().map(|l| l.mid).variance();
        debug!("variance {:?}", variance);
        let x = self.rows.clone().into_iter();
        let covariance: f64 = x
            .map(|r| (r.left.map(|o| o.mid), r.right.map(|o| o.mid)))
            .take_while(|p| p.0.is_some() && p.1.is_some())
            .map(|p| (p.0.unwrap(), p.1.unwrap())).covariance::<(f64, f64), f64>();
        debug!("covariance {:?}", covariance);
        let beta_val = covariance / variance;
        debug!("beta_val {:?}", beta_val);
        beta_val
    }
}


#[cfg(test)]
mod test {
    use stats::stddev;
    use std::fs::File;
    use crate::strategies::naive_pair_trading::{DataTable, DataRow, BookPosition};
    use std::io::Result;
    use serde::{Serialize, Deserialize};
    use chrono::{DateTime, Utc};
    use chrono::serde::ts_seconds;
    use std::rc::Rc;
    use std::borrow::BorrowMut;
    use std::cell::RefCell;
    use itertools::Itertools;
    use crate::serdes::date_time_format;

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
        let mut dt = DataTable {
            rows: Vec::new()
        };
        let vec: Vec<CsvRecord> = rdr.deserialize().map(|r| {
            let record: Result<CsvRecord> = r.map_err(|e| e.into());
            record.ok()
        }).while_some().collect(); // just skip invalid rows
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

    #[test]
    fn default_scenario_test_loop() {
        // pseudo code
        // for i in ((beta_eval_window_size + 1) .. pair_data.size)
        //
    }

    #[test]
    fn variance() {
        let mut dt = DataTable {
            rows: Vec::new()
        };
        let eval_window_size = 500;
        // Read downsampled streams
        let dt1 = read_csv("/Users/geps/dev/bitcoin/bitcoins/data/Binance/order_books/pr=ETH_USDT/dt=2020-03-25.csv").unwrap();
        let dt1_iter: Vec<CsvRecord> = dt1.into_iter().group_by(|r| r.hourofday.timestamp()).into_iter().take(eval_window_size).map(|(k, mut v)| {
            v.next().unwrap()
        }).collect();
        let dt2 = read_csv("/Users/geps/dev/bitcoin/bitcoins/data/Binance/order_books/pr=BTC_USDT/dt=2020-03-25.csv").unwrap();
        let dt2_iter: Vec<CsvRecord> = dt2.into_iter().group_by(|r| r.hourofday.timestamp()).into_iter().take(eval_window_size).map(|(k, mut v)| {
            v.next().unwrap()
        }).collect();
        // align data
        interleave_crypto_signals(&mut dt, dt1_iter, dt2_iter);
        assert!(dt.beta_val() > 0.0);
    }

    fn interleave_crypto_signals(dt: &mut DataTable, dt1_iter: Vec<CsvRecord>, dt2_iter: Vec<CsvRecord>) {
        let mut peekable = dt1_iter.into_iter().peekable();
        let left_p = peekable.by_ref();
        let mut peekable1 = dt2_iter.into_iter().peekable();
        let right_p = peekable1.by_ref();
        loop {
            let left = left_p.peek();
            let right = right_p.peek();
            match (left, right) {
                (None, None) => break, //end of stream
                (Some(l), None) => {
                    //nothing left in right stream
                    left_p.for_each(|r| dt.push(DataRow {
                        time: r.time(),
                        left: Some(to_pos(r)),
                        right: None,
                    }))
                }
                (None, Some(r)) => {
                    //nothing left in right stream
                    right_p.for_each(|r| dt.push(DataRow {
                        time: r.time(),
                        left: Some(to_pos(r)),
                        right: None,
                    }))
                }
                (Some(l), Some(r)) => {
                    let left_time = l.time();
                    let right_time = r.time();
                    // streams are aligned, take and continue
                    if left_time == right_time {
                        dt.push(DataRow {
                            time: left_time,
                            left: Some(to_pos(left_p.next().unwrap())),
                            right: Some(to_pos(right_p.next().unwrap())),
                        });
                        continue;
                    }
                    // right is late
                    else if left_time > right_time {
                        {
                            right_p.take_while(|r| r.time() < left_time).for_each(|r|
                                dt.push(DataRow {
                                    time: r.time(),
                                    left: None,
                                    right: Some(to_pos(r)),
                                })
                            );
                        }
                    }
                    // left is late
                    else if left_time < right_time {
                        {
                            left_p.take_while(|r| left_time < right_time).for_each(|r|
                                dt.push(DataRow {
                                    time: r.time(),
                                    left: Some(to_pos(r)),
                                    right: None,
                                })
                            );
                        }
                    }
                }
            }
        }
    }
}
