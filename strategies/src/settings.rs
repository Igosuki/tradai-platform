use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use itertools::Itertools;

use coinnect_rt::pair::filter_pairs;
use coinnect_rt::prelude::*;
use db::{get_or_create, Storage};
use trading::engine::TradingEngine;

use crate::driver::StrategyDriver;
use crate::generic::{GenericDriverOptions, Strategy};
use crate::mean_reverting::options::Options as MeanRevertingStrategyOptions;
use crate::naive_pair_trading::options::Options as NaiveStrategyOptions;
use crate::{error, generic, DbOptions, StratEventLogger, StrategyKey, StrategyType};

/// Strategy configuration
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum StrategySettings {
    Naive(NaiveStrategyOptions),
    MeanReverting(MeanRevertingStrategyOptions),
    Generic {
        strat: Box<StrategySettings>,
        driver: GenericDriverOptions,
    },
}

impl StrategySettings {
    pub fn exchange(&self) -> Exchange {
        match self {
            Self::Naive(s) => s.exchange,
            Self::MeanReverting(s) => s.exchange,
            Self::Generic { strat: s, .. } => s.exchange(),
        }
    }

    pub fn key(&self) -> StrategyKey {
        match &self {
            Self::Naive(n) => StrategyKey(StrategyType::Naive, format!("{}_{}", n.left, n.right)),
            Self::MeanReverting(n) => StrategyKey(StrategyType::MeanReverting, format!("{}", n.pair)),
            Self::Generic { strat: s, .. } => s.key(),
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
            StrategySettings::Generic { strat: s, driver } => s
                .replicate_for_pairs(pairs)
                .into_iter()
                .map(|s| Self::Generic {
                    driver: driver.clone(),
                    strat: Box::new(s),
                })
                .collect(),
        }
    }
}

/// Handles replicating strategy configurations
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
    db_opts: &DbOptions<S>,
    exchange_conf: &ExchangeSettings,
    s: &StrategySettings,
    engine: Arc<TradingEngine>,
    logger: Option<Arc<dyn StratEventLogger>>,
) -> Box<dyn StrategyDriver> {
    let strat_key = s.key().to_string();
    let db = get_or_create(db_opts, strat_key.clone(), vec![]);
    match s {
        StrategySettings::Naive(n) => Box::new(crate::naive_pair_trading::NaiveTradingStrategy::new(
            db,
            strat_key,
            exchange_conf.fees,
            n,
            engine,
            logger,
        )),
        StrategySettings::MeanReverting(n) => Box::new(crate::mean_reverting::MeanRevertingStrategy::new(
            db,
            strat_key,
            exchange_conf.fees,
            n,
            engine,
            logger,
        )),
        StrategySettings::Generic { strat: s, driver } => {
            let inner: Box<dyn generic::Strategy> =
                from_settings_s(db.clone(), strat_key, exchange_conf, s, engine.clone(), logger);
            Box::new(
                crate::generic::GenericDriver::try_new(
                    inner.channels().into_iter().collect(),
                    db,
                    driver,
                    inner,
                    engine,
                )
                .unwrap(),
            )
        }
    }
}

pub(crate) fn from_settings_s(
    db: Arc<dyn Storage>,
    key: String,
    exchange_conf: &ExchangeSettings,
    s: &StrategySettings,
    engine: Arc<TradingEngine>,
    logger: Option<Arc<dyn StratEventLogger>>,
) -> Box<dyn Strategy> {
    match s {
        StrategySettings::MeanReverting(n) => Box::new(crate::mean_reverting::MeanRevertingStrategy::new(
            db,
            key,
            exchange_conf.fees,
            n,
            engine,
            logger,
        )),
        _ => panic!(),
    }
}
