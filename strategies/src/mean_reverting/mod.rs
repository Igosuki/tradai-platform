use crate::mean_reverting::ema_model::SinglePosRow;
use crate::mean_reverting::metrics::MeanRevertingStrategyMetrics;
use crate::mean_reverting::options::Options;
use crate::ob_double_window_model::DoubleWindowTable;
use crate::order_manager::OrderManager;
use actix::Addr;
use db::Db;
use std::path::PathBuf;

mod ema_model;
mod metrics;
mod options;

pub struct MeanRevertingState {}

pub struct MeanRevertingStrategy {
    fees_rate: f64,
    db: Db,
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
        Self { fees_rate, db }
    }

    fn long_position_return(&self, traded_price: f64, current_price: f64, units: f64) -> f64 {
        units * (current_price * (1.0 - self.fees_rate) - traded_price) / (units * traded_price)
    }

    fn short_position_return(&self, traded_price: f64, current_price: f64, units: f64) -> f64 {
        units * (traded_price - current_price * (1.0 + self.fees_rate)) / (units * traded_price)
    }

    pub fn make_model_table(
        pair: &str,
        db_path: &str,
        short_window_size: usize,
        long_window_size: usize,
    ) -> DoubleWindowTable<SinglePosRow> {
        DoubleWindowTable::new(
            &pair,
            db_path,
            short_window_size,
            long_window_size,
            Box::new(ema_model::moving_average),
        )
    }
}

#[cfg(test)]
mod test {
    use crate::input::{self, to_pos};
    use crate::mean_reverting::ema_model::SinglePosRow;
    use crate::mean_reverting::MeanRevertingStrategy;
    use chrono::{TimeZone, Utc};
    use util::date::{DateRange, DurationRangeType};

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    static exchange_name: &str = "Binance";
    static channel: &str = "order_books";
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
            exchange_name,
            channel,
        )
        .await;
        // align data
        records[0]
            .iter()
            .zip(records[1].iter())
            .take(500)
            .for_each(|(l, r)| {
                dt.push(&SinglePosRow {
                    time: l.event_ms,
                    pos: to_pos(r),
                })
            });
        let x = dt.moving_avg();
        println!("beta {}", x);
        assert!(x > 0.0, x);
    }
}
