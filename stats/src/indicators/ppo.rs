use ta::{Close, Next};

use crate::indicators::ema::ExponentialMovingAverage;

#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
pub struct PercentPriceOscillator {
    pub long_ema: ExponentialMovingAverage,
    pub short_ema: ExponentialMovingAverage,
    pub ppo: f64,
}

impl PercentPriceOscillator {
    #[allow(clippy::missing_panics_doc)]
    pub fn new(long_window: u32, short_window: u32) -> PercentPriceOscillator {
        PercentPriceOscillator {
            long_ema: ExponentialMovingAverage::new(2.0, long_window).unwrap(),
            short_ema: ExponentialMovingAverage::new(2.0, short_window).unwrap(),
            ppo: 0.0,
        }
    }
}

impl Next<f64> for PercentPriceOscillator {
    type Output = f64;

    fn next(&mut self, input: f64) -> Self::Output {
        let long_ema = self.long_ema.next(input);
        let short_ema = self.short_ema.next(input);
        self.ppo = (short_ema - long_ema) / long_ema;

        self.ppo
    }
}

impl<R: Close> Next<&R> for PercentPriceOscillator {
    type Output = f64;

    fn next(&mut self, input: &R) -> Self::Output {
        let v = input.close();
        self.next(v)
    }
}
