use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use itertools::Itertools;

use coinnect_rt::pair::filter_pairs;
use coinnect_rt::prelude::*;
use db::{get_or_create, Storage};
use trading::engine::TradingEngine;

use crate::driver::{Strategy, StrategyDriver};
use crate::generic::GenericDriverOptions;
use crate::mean_reverting::options::Options as MeanRevertingStrategyOptions;
use crate::naive_pair_trading::options::Options as NaiveStrategyOptions;
use crate::{error, DbOptions, StratEventLogger, StrategyKey, StrategyType};

/// Strategy configuration
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum StrategySettings {
    Naive(NaiveStrategyOptions),
    MeanReverting(MeanRevertingStrategyOptions),
}

impl StrategySettings {
    pub fn exchange(&self) -> Exchange {
        match self {
            Self::Naive(s) => s.exchange,
            Self::MeanReverting(s) => s.exchange,
        }
    }

    pub fn key(&self) -> StrategyKey {
        match &self {
            Self::Naive(n) => StrategyKey(StrategyType::Naive, format!("{}_{}", n.left, n.right)),
            Self::MeanReverting(n) => StrategyKey(StrategyType::MeanReverting, format!("{}", n.pair)),
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
        }
    }
}

/// Handles replicating strategy configurations
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum StrategyCopySettings {
    /// Replicates the strategy for all available markets on the target exchange
    MarketReplica {
        pairs: Vec<String>,
        base: StrategyDriverSettings,
    },
}

impl StrategyCopySettings {
    pub fn exchange(&self) -> Exchange {
        match self {
            StrategyCopySettings::MarketReplica {
                base: StrategyDriverSettings { strat, .. },
                ..
            } => strat.exchange(),
        }
    }
    pub fn all(&self) -> error::Result<Vec<StrategyDriverSettings>> {
        match self {
            StrategyCopySettings::MarketReplica {
                pairs,
                base: StrategyDriverSettings { strat, driver },
            } => {
                let pairs = filter_pairs(&strat.exchange(), pairs)?;
                Ok(strat
                    .replicate_for_pairs(pairs)
                    .into_iter()
                    .map(|replica| StrategyDriverSettings {
                        driver: driver.clone(),
                        strat: Box::new(replica),
                    })
                    .collect())
            }
        }
    }
}

pub(crate) fn from_settings(
    strat_key: String,
    db: Arc<dyn Storage>,
    exchange_conf: &ExchangeSettings,
    s: &StrategySettings,
    engine: Arc<TradingEngine>,
    logger: Option<Arc<dyn StratEventLogger>>,
) -> Box<dyn Strategy> {
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
    }
}

/// Strategy driver option types
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum StrategyDriverOptions {
    Generic(GenericDriverOptions),
}

/// Strategy driver
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub struct StrategyDriverSettings {
    pub strat: Box<StrategySettings>,
    pub driver: StrategyDriverOptions,
}

impl StrategyDriverSettings {
    pub fn exchange(&self) -> Exchange { self.strat.exchange() }

    pub fn key(&self) -> StrategyKey { self.strat.key() }
}

pub fn from_driver_settings<S: AsRef<Path>>(
    db_opts: &DbOptions<S>,
    exchange_conf: &ExchangeSettings,
    s: &StrategyDriverSettings,
    engine: Arc<TradingEngine>,
    logger: Option<Arc<dyn StratEventLogger>>,
) -> Box<dyn StrategyDriver> {
    let strat_key = s.strat.key().to_string();
    let db = get_or_create(db_opts, strat_key.clone(), vec![]);
    let inner: Box<dyn crate::driver::Strategy> = from_settings(
        strat_key,
        db.clone(),
        exchange_conf,
        s.strat.as_ref(),
        engine.clone(),
        logger,
    );
    match &s.driver {
        StrategyDriverOptions::Generic(options) => Box::new(
            crate::generic::GenericDriver::try_new(inner.channels().into_iter().collect(), db, options, inner, engine)
                .unwrap(),
        ),
    }
}
