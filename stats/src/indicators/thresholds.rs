use std::cmp::{max, min};

use itertools::Itertools;
use ordered_float::OrderedFloat;
use ta::Next;

use crate::iter::QuantileExt;

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Default)]
pub struct Thresholds {
    pub high_0: f64,
    pub low_0: f64,
    pub low: f64,
    pub high: f64,
}

impl Thresholds {
    pub fn new(high_0: f64, low_0: f64) -> Self {
        Self {
            high_0,
            low_0,
            low: 0.0,
            high: 0.0,
        }
    }
}

impl<'a, I: Iterator<Item = &'a f64>> Next<I> for Thresholds {
    type Output = (f64, f64);

    fn next(&mut self, input: I) -> Self::Output {
        let (high_iter, low_iter) = input.tee();
        let high = max(self.high_0.into(), OrderedFloat(high_iter.quantile(0.99))).into();
        let low = min(self.low_0.into(), OrderedFloat(low_iter.quantile(0.01))).into();
        self.high = high;
        self.low = low;
        (low, high)
    }
}
