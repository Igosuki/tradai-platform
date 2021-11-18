use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use actix::Addr;
use itertools::Itertools;

use coinnect_rt::margin_interest_rates::MarginInterestRateProvider;
use coinnect_rt::pair::filter_pairs;
use coinnect_rt::prelude::*;

use crate::driver::StrategyDriver;
use crate::generic::Strategy;
use crate::mean_reverting::options::Options as MeanRevertingStrategyOptions;
use crate::naive_pair_trading::options::Options as NaiveStrategyOptions;
use crate::order_manager::OrderManager;
use crate::{error, generic, DbOptions, StratEventLogger, StrategyKey, StrategyType};

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

    pub fn replicate_for_pairs(&self, pairs: HashSet<Pair>) -> Vec<Self> {
        match self {
            StrategySettings::Naive(o) => {
                let pair_pairs = pairs.into_iter().permutations(2);
                pair_pairs
                    .into_iter()
                    .map(|pair_pair| {
                        let mut new = o.clone();
                        new.left = pair_pair[0].clone();
                        new.right = pair_pair[1].clone();
                        Self::Naive(new)
                    })
                    .collect()
            }
            StrategySettings::MeanReverting(o) => pairs
                .into_iter()
                .map(|pair| {
                    let mut new = o.clone();
                    new.pair = pair;
                    Self::MeanReverting(new)
                })
                .collect(),
            StrategySettings::Generic(s) => s
                .replicate_for_pairs(pairs)
                .into_iter()
                .map(|s| Self::Generic(Box::new(s)))
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum StrategyCopySettings {
    /// Replicates the strategy for all available markets on the target exchange
    MarketReplica { pairs: Vec<String>, base: StrategySettings },
}

impl StrategyCopySettings {
    pub fn exchange(&self) -> Exchange {
        match self {
            StrategyCopySettings::MarketReplica { base, .. } => base.exchange(),
        }
    }
    pub fn all(&self) -> error::Result<Vec<StrategySettings>> {
        match self {
            StrategyCopySettings::MarketReplica { pairs, base } => {
                let pairs = filter_pairs(&base.exchange(), pairs)?;
                Ok(base.replicate_for_pairs(pairs))
            }
        }
    }
}

pub fn from_settings<S: AsRef<Path>>(
    db: &DbOptions<S>,
    exchange_conf: &ExchangeSettings,
    s: &StrategySettings,
    om: Option<Addr<OrderManager>>,
    mirp: Addr<MarginInterestRateProvider>,
    logger: Option<Arc<dyn StratEventLogger>>,
) -> Box<dyn StrategyDriver> {
    match s {
        StrategySettings::Naive(n) => {
            if let Some(o) = om {
                Box::new(crate::naive_pair_trading::NaiveTradingStrategy::new(
                    db,
                    exchange_conf.fees,
                    n,
                    o,
                ))
            } else {
                log::error!(
                    "Expected an order manager to be available for the targeted exchange of this NaiveStrategy"
                );
                panic!();
            }
        }
        StrategySettings::MeanReverting(n) => {
            if let Some(o) = om {
                Box::new(crate::mean_reverting::MeanRevertingStrategy::new(
                    db,
                    exchange_conf.fees,
                    n,
                    o,
                    mirp,
                    logger,
                ))
            } else {
                log::error!(
                    "Expected an order manager to be available for the targeted exchange of this MeanRevertingStrategy"
                );
                panic!();
            }
        }
        StrategySettings::Generic(s) => {
            let inner: Box<dyn generic::Strategy> = from_settings_s(db, exchange_conf, s, om, mirp, logger);
            Box::new(crate::generic::GenericStrategy::try_new(inner.channels().into_iter().collect(), inner).unwrap())
        }
    }
}

pub(crate) fn from_settings_s<S: AsRef<Path>>(
    db: &DbOptions<S>,
    exchange_conf: &ExchangeSettings,
    s: &StrategySettings,
    om: Option<Addr<OrderManager>>,
    mirp: Addr<MarginInterestRateProvider>,
    logger: Option<Arc<dyn StratEventLogger>>,
) -> Box<dyn Strategy> {
    match s {
        StrategySettings::MeanReverting(n) => {
            if let Some(o) = om {
                Box::new(crate::mean_reverting::MeanRevertingStrategy::new(
                    db,
                    exchange_conf.fees,
                    n,
                    o,
                    mirp,
                    logger,
                ))
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
