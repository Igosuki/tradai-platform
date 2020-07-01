use crate::mean_reverting::metrics::MeanRevertingStrategyMetrics;
use crate::mean_reverting::options::Options;
use crate::order_manager::OrderManager;
use actix::Addr;
use db::Db;
use std::path::PathBuf;

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
}
