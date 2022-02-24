use yata::core::Source;
use yata::helpers::MA;
use yata::indicators::{StochasticOscillator, MACD, RSI};

pub mod cross;
pub mod drawdown;
pub mod ema;
pub mod momentum;
pub mod ppo;
pub mod ppo_yata;
pub mod ratio;
pub mod thresholds;

pub fn stoch(period: u32, smooth_k: u32, signal: u32, zone_low: f64) -> StochasticOscillator {
    StochasticOscillator {
        period,
        ma: MA::SMA(smooth_k),
        signal: MA::SMA(signal),
        zone: zone_low,
    }
}

pub fn macd(source: Source, macd_fast: u32, macd_slow: u32, macd_signal: u32) -> MACD {
    MACD {
        source,
        ma1: MA::EMA(macd_fast),
        ma2: MA::EMA(macd_slow),
        signal: MA::SMA(macd_signal),
    }
}

pub fn rsi(source: Source, rsi_len: u32, rsi_low: f64) -> RSI {
    RSI {
        ma: MA::RMA(rsi_len),
        zone: rsi_low,
        source,
    }
}
