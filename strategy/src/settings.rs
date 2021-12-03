use std::collections::HashSet;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use serde_json::Value;

use coinnect_rt::pair::filter_pairs;
use coinnect_rt::prelude::*;
use db::{get_or_create, DbOptions};
use trading::engine::TradingEngine;

use crate::driver::StrategyDriver;
use crate::generic::GenericDriverOptions;
use crate::plugin::{gather_plugins, StrategyPlugin, StrategyPluginContext};
use crate::{error::Result, Error, StratEventLogger, StrategyKey};

/// Strategy configuration
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct StrategySettings {
    #[serde(rename = "type")]
    pub strat_type: String,
    #[serde(flatten)]
    pub options: Value,
}

pub trait StrategySettingsReplicator {
    fn replicate_for_pairs(&self, pairs: HashSet<Pair>) -> Vec<Value>;
}

pub trait StrategyOptions: StrategySettingsReplicator {
    fn key(&self) -> StrategyKey;
}

/// Handles replicating strategy configurations
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum StrategyCopySettings {
    /// Replicates the strategy for all available markets on the target exchange
    MarketReplica {
        pairs: Vec<String>,
        exchanges: Vec<String>,
        base: StrategyDriverSettings,
    },
}

impl StrategyCopySettings {
    pub fn exchanges(&self) -> Vec<Exchange> {
        match self {
            StrategyCopySettings::MarketReplica { exchanges, .. } => exchanges
                .iter()
                .map(|s| Exchange::from_str(s.as_str()).unwrap())
                .collect(),
        }
    }
    pub fn all(&self) -> Result<Vec<StrategyDriverSettings>> {
        match self {
            StrategyCopySettings::MarketReplica {
                pairs,
                exchanges,
                base: StrategyDriverSettings { strat, driver },
            } => {
                let plugin_registry = gather_plugins();
                let plugin = plugin_registry
                    .get(strat.strat_type.as_str())
                    .ok_or(Error::StrategyPluginNotFound)?;
                let conf = plugin.options(strat.options.clone())?;
                let known_exchanges = exchanges.iter().filter_map(|s| Exchange::from_str(s.as_str()).ok());
                let strats = known_exchanges
                    .into_iter()
                    .map(|exchange| {
                        let pairs = filter_pairs(&exchange, pairs).unwrap();
                        conf.replicate_for_pairs(pairs)
                            .into_iter()
                            .map(|replica| StrategyDriverSettings {
                                driver: driver.clone(),
                                strat: Box::new(StrategySettings {
                                    options: replica,
                                    strat_type: strat.strat_type.clone(),
                                }),
                            })
                    })
                    .flatten()
                    .collect();

                Ok(strats)
            }
        }
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

pub fn from_driver_settings<S: AsRef<Path>>(
    plugin: &StrategyPlugin,
    db_opts: &DbOptions<S>,
    _exchange_conf: &ExchangeSettings,
    s: &StrategyDriverSettings,
    engine: Arc<TradingEngine>,
    logger: Option<Arc<dyn StratEventLogger>>,
) -> Result<Box<dyn StrategyDriver>> {
    let key = plugin.options(s.strat.options.clone())?.key();
    let strat_key = key.to_string();
    let db = get_or_create(db_opts, strat_key.clone(), vec![]);
    let ctx = StrategyPluginContext::builder()
        .db(db.clone())
        .engine(engine.clone())
        .logger(logger)
        .build();
    let inner: Box<dyn crate::driver::Strategy> = plugin.strat(&strat_key, ctx, s.strat.options.clone())?;

    let driver = match &s.driver {
        StrategyDriverOptions::Generic(options) => Box::new(
            crate::generic::GenericDriver::try_new(inner.channels().into_iter().collect(), db, options, inner, engine)
                .unwrap(),
        ),
    };
    info!("Creating strategy : {}", strat_key);
    Ok(driver)
}
