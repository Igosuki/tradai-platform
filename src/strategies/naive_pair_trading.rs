use stats::stddev;

pub struct Strategy {
    initial_value_strat: i32,
    fees_rate: f64,
    res_threshold_long: f64,
    res_threshold_short: f64,
    stop_loss: f64,
    beta_eval_window_size: i32,
    beta_eval_freq: i32,
    position_short: i8,
    position_long: i8,
    value_strat: i32
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
            position_short: 0,
            position_long: 0,
            value_strat: 100,
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

}

struct BookPosition {
    mid: f64, // mid = (top_ask + top_bid) / 2, alias: crypto1_m
    top_ask: f64, // crypto_a
    top_ask_q: f64, // crypto_a_q
    top_bid: f64, // crypto_b
    top_bid_q: f64, // crypto_b_q
    log_r: f64, // crypto_log_r
    log_r_10: f64, // crypto_log_r
}

struct DataRow {
    time: u64,
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

