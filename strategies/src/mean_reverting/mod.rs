use crate::mean_reverting::options::Options;
use crate::order_manager::OrderManager;
use actix::Addr;

mod options;

pub struct MeanRevertingStrategy {}

impl MeanRevertingStrategy {
    pub fn new(db_path: &str, fees_rate: f64, n: &Options, om: Addr<OrderManager>) -> Self {
        MeanRevertingStrategy {}
    }
}
