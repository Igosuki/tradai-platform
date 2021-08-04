use crate::StrategySettings;

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("configuration error")]
    ConfError,
}

type Result<T> = std::result::Result<T, Error>;

struct Backtest;

struct BacktestConfig {
    strat: StrategySettings,
}

impl Backtest {
    fn new(_conf: &BacktestConfig) -> Result<Self> { Ok(Self) }
}
