use std::cmp::{max, min};

use ordered_float::OrderedFloat;
use ta::Next;

use crate::iter::QuantileExt;

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Default)]
pub struct Thresholds {
    // TODO: temporarily for deserialization
    #[serde(alias = "short_0")]
    pub high_0: f64,
    #[serde(alias = "long_0")]
    pub low_0: f64,
    #[serde(alias = "long")]
    pub low: f64,
    #[serde(alias = "short")]
    pub high: f64,
}

impl Thresholds {
    pub fn new(high_0: f64, low_0: f64) -> Self {
        Self {
            high_0,
            low_0,
            low: low_0,
            high: high_0,
        }
    }
}

const QUANTILES: [f64; 2] = [0.01, 0.99];

impl<'a, I: Iterator<Item = &'a f64>> Next<I> for Thresholds {
    type Output = (f64, f64);

    fn next(&mut self, input: I) -> Self::Output {
        let quants: Vec<f64> = input.copied().quantiles::<f64>(QUANTILES.to_vec());
        let high = max(self.high_0.into(), OrderedFloat(quants[1])).into();
        let low = min(self.low_0.into(), OrderedFloat(quants[0])).into();
        self.high = high;
        self.low = low;
        (low, high)
    }
}
