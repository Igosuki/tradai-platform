use actix::Addr;

use coinnect_rt::exchange::Exchange;
use coinnect_rt::pair::filter_pairs;

use crate::generic::Strategy;
use crate::mean_reverting::options::Options as MeanRevertingStrategyOptions;
use crate::naive_pair_trading::options::Options as NaiveStrategyOptions;
use crate::order_manager::OrderManager;
use crate::{error, generic, DbOptions, StrategyDriver, StrategyKey, StrategyType};

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum StrategySettings {
    Naive(NaiveStrategyOptions),
    MeanReverting(MeanRevertingStrategyOptions),
    Generic(Box<StrategySettings>),
}

impl StrategySettings {
    pub fn exchange(&self) -> Exchange {
        match self {
            Self::Naive(s) => s.exchange,
            Self::MeanReverting(s) => s.exchange,
            Self::Generic(s) => s.exchange(),
        }
    }

    pub fn key(&self) -> StrategyKey {
        match &self {
            StrategySettings::Naive(n) => StrategyKey(StrategyType::Naive, format!("{}_{}", n.left, n.right)),
            StrategySettings::MeanReverting(n) => StrategyKey(StrategyType::MeanReverting, format!("{}", n.pair)),
            StrategySettings::Generic(s) => s.key(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum StrategyCopySettings {
    MeanReverting {
        pairs: Vec<String>,
        base: MeanRevertingStrategyOptions,
    },
}

impl StrategyCopySettings {
    pub fn exchange(&self) -> Exchange {
        match self {
            Self::MeanReverting { base, .. } => base.exchange,
        }
    }

    pub fn all(&self) -> error::Result<Vec<StrategySettings>> {
        match self {
            StrategyCopySettings::MeanReverting { pairs, base } => {
                let pairs = filter_pairs(&self.exchange(), pairs)?;
                Ok(pairs
                    .into_iter()
                    .map(|pair| StrategySettings::MeanReverting(MeanRevertingStrategyOptions { pair, ..base.clone() }))
                    .collect())
            }
        }
    }
}

pub fn from_settings(
    db: &DbOptions<String>,
    fees: f64,
    s: &StrategySettings,
    om: Option<Addr<OrderManager>>,
) -> Box<dyn StrategyDriver> {
    match s {
        StrategySettings::Naive(n) => {
            if let Some(o) = om {
                Box::new(crate::naive_pair_trading::NaiveTradingStrategy::new(db, fees, n, o))
            } else {
                log::error!(
                    "Expected an order manager to be available for the targeted exchange of this NaiveStrategy"
                );
                panic!();
            }
        }
        StrategySettings::MeanReverting(n) => {
            if let Some(o) = om {
                Box::new(crate::mean_reverting::MeanRevertingStrategy::new(db, fees, n, o))
            } else {
                log::error!(
                    "Expected an order manager to be available for the targeted exchange of this MeanRevertingStrategy"
                );
                panic!();
            }
        }
        StrategySettings::Generic(s) => {
            let inner: Box<dyn generic::Strategy> = from_settings_s(db, fees, s, om);
            Box::new(crate::generic::GenericStrategy::try_new(inner.channels().into_iter().collect(), inner).unwrap())
        }
    }
}

pub(crate) fn from_settings_s(
    db: &DbOptions<String>,
    fees: f64,
    s: &StrategySettings,
    om: Option<Addr<OrderManager>>,
) -> Box<dyn Strategy> {
    match s {
        StrategySettings::MeanReverting(n) => {
            if let Some(o) = om {
                Box::new(crate::mean_reverting::MeanRevertingStrategy::new(db, fees, n, o))
            } else {
                log::error!(
                    "Expected an order manager to be available for the targeted exchange of this MeanRevertingStrategy"
                );
                panic!();
            }
        }
        _ => panic!(),
    }
}
