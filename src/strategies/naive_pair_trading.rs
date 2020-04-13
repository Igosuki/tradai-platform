use stats::stddev;
use average::Mean;

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
    pub evals_so_far: i32
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
        }
    }

    fn get_long_position_value(&self, traded_price_crypto2: f64, traded_price_crypto1: f64, current_price_crypto2: f64, current_price_crypto1: f64, beta_val: f64, units_to_buy: f64) -> f64 {
        return units_to_buy * (traded_price_crypto1*beta_val * (1.0 - self.fees_rate) - traded_price_crypto2 * (1.0 + self.fees_rate) + current_price_crypto2 * (1.0 - self.fees_rate) - current_price_crypto1 * beta_val * (1.0 + self.fees_rate))
    }

    fn get_long_position_return(&self, traded_price_crypto2: f64, traded_price_crypto1: f64, current_price_crypto2: f64, current_price_crypto1: f64, beta_val: f64, units_to_buy: f64, value_strat: f64) -> f64 {
        return self.get_long_position_value(traded_price_crypto2, traded_price_crypto1, current_price_crypto2, current_price_crypto1, beta_val, units_to_buy) / value_strat
    }

    fn get_short_position_value(&self, traded_price_crypto2: f64, traded_price_crypto1: f64, current_price_crypto2: f64, current_price_crypto1: f64, beta_val: f64, units_to_buy: f64) -> f64{
        return units_to_buy * (traded_price_crypto2 * (1.0 - self.fees_rate) - traded_price_crypto1*beta_val * (1.0 + self.fees_rate) + current_price_crypto1 * beta_val * (1.0 - self.fees_rate) - current_price_crypto2 * (1.0 + self.fees_rate))
    }

    fn get_short_position_return(&self, traded_price_crypto2: f64, traded_price_crypto1: f64, current_price_crypto2: f64, current_price_crypto1: f64, beta_val: f64, units_to_buy: f64, value_strat: f64) -> f64 {
        return self.get_short_position_value(traded_price_crypto2, traded_price_crypto1, current_price_crypto2, current_price_crypto1, beta_val, units_to_buy) / value_strat
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

struct BookPosition {
    mid: f64, // mid = (top_ask + top_bid) / 2, alias: crypto1_m
    top_ask: f64, // crypto_a
    top_ask_q: f64, // crypto_a_q
    top_bid: f64, // crypto_b
    top_bid_q: f64, // crypto_b_q
    // log_r: f64, // crypto_log_r
    // log_r_10: f64, // crypto_log_r
}

impl BookPosition {
    fn mid(&self) -> f64 {
        (self.top_ask + self.top_bid) / 2.0
    }
}

const DATE_FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";

struct DataRow {
    time: i64,
    left: BookPosition, // crypto_1
    right: BookPosition // crypto_2
}

struct DataTable {
    rows: Vec<DataRow>,
}

impl DataTable {
    fn log_r_std_10(&mut self) {
        // self.stddev(self.rows.into_iter().map(|r| r.left.mid.log(2.0)));
        // stddev(self.rows.into_iter().map(|r| r.right.mid.log(2.0)));
    }
}


#[cfg(test)]
mod test {
    use stats::stddev;
    use std::fs::File;
    use crate::strategies::naive_pair_trading::{DataTable, DataRow, DATE_FORMAT, BookPosition};
    use std::io::Result;
    use serde::{Serialize, Deserialize};
    use chrono::{DateTime, Utc};
    use chrono::serde::ts_seconds;
    use itertools::Itertools;

    mod my_date_format {
        use chrono::{DateTime, Utc, TimeZone};
        use serde::{self, Deserialize, Serializer, Deserializer};
        use crate::strategies::naive_pair_trading::DATE_FORMAT;

        pub fn serialize<S>(
            date: &DateTime<Utc>,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
        {
            let s = format!("{}", date.format(DATE_FORMAT));
            serializer.serialize_str(&s)
        }

        pub fn deserialize<'de, D>(
            deserializer: D,
        ) -> Result<DateTime<Utc>, D::Error>
            where
                D: Deserializer<'de>,
        {
            let s = String::deserialize(deserializer)?;
            Utc.datetime_from_str(&s, DATE_FORMAT).map_err(serde::de::Error::custom)
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct CsvRecord {
        #[serde(with = "my_date_format")]
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
        bq5: f64
    }

    fn read_csv(path: &str) -> Result<Vec<CsvRecord>> {
        let f = File::open(path)?;
        let mut rdr = csv::Reader::from_reader(f);
        let mut dt = DataTable {
            rows: Vec::new()
        };
        let vec: Vec<CsvRecord> = rdr.deserialize().map(|r| {
            let record: Result<CsvRecord> = r.map_err(|e| e.into());
            record
        }).filter(|r| r.is_ok()).map(|r| r.unwrap()).collect();
        Ok(vec)
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
        // dt.rows.push(DataRow {
        //     time: DateTime::parse_from_str(&record.hourofday, DATE_FORMAT).unwrap().timestamp(),
        //     left: BookPosition {
        //
        //     }
        // })
        let dt1 = read_csv("/Users/geps/dev/bitcoin/bitcoins/data/Binance/order_books/pr=ETH_USDT/dt=2020-03-25.csv").unwrap();
        let dt1_iter: Vec<CsvRecord> = dt1.into_iter().group_by(|r| r.hourofday.timestamp()).into_iter().take(10).map(|(k, mut v)| {
            v.next().unwrap()
        }).collect();
        let dt2 = read_csv("/Users/geps/dev/bitcoin/bitcoins/data/Binance/order_books/pr=BTC_USDT/dt=2020-03-25.csv").unwrap();
        let dt2_iter: Vec<CsvRecord> = dt2.into_iter().group_by(|r| r.hourofday.timestamp()).into_iter().take(10).map(|(k, mut v)| {
            v.next().unwrap()
        }).collect();
        // align data
        let mut left_p = dt1_iter.into_iter().peekable();
        let mut right_p = dt2_iter.into_iter().peekable();
        loop {
            let left = left_p.peek();
            let right = right_p.peek();
            println!("{:?}, {:?}", left, right);
            break;
            // let records: Vec<CsvRecord> = dt1_iter.peeking_take_while(|r| right.is_none() || r.hourofday <= right.unwrap().hourofday).into();
        }
    }
}
