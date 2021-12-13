use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value;

use db::Storage;
use trading::engine::TradingEngine;

use crate::driver::Strategy;
use crate::error::Result;
use crate::settings::StrategyOptions;
use crate::StratEventLogger;

pub(crate) type StratProvider = fn(&str, StrategyPluginContext, serde_json::Value) -> Result<Box<dyn Strategy>>;
pub(crate) type OptionsProvider = fn(serde_json::Value) -> Result<Box<dyn StrategyOptions>>;

pub fn provide_options<T: 'static + DeserializeOwned + StrategyOptions>(
    conf: serde_json::Value,
) -> Result<Box<dyn StrategyOptions>> {
    let options: T = serde_json::from_value(conf)?;
    Ok(Box::new(options))
}

#[derive(typed_builder::TypedBuilder)]
pub struct StrategyPluginContext {
    pub db: Arc<dyn Storage>,
    pub engine: Arc<TradingEngine>,
    pub logger: Option<Arc<dyn StratEventLogger>>,
}

pub struct StrategyPlugin {
    name: &'static str,
    provider: StratProvider,
    opt_provider: OptionsProvider,
}

impl Debug for StrategyPlugin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StrategyPlugin").field("name", &self.name).finish()
    }
}

impl StrategyPlugin {
    pub const fn new(name: &'static str, key_provider: OptionsProvider, provider: StratProvider) -> Self {
        Self {
            name,
            provider,
            opt_provider: key_provider,
        }
    }

    pub fn options(&self, conf: Value) -> Result<Box<dyn StrategyOptions>> { (self.opt_provider)(conf) }

    pub fn strat(&self, key: &str, ctx: StrategyPluginContext, conf: Value) -> Result<Box<dyn Strategy>> {
        (self.provider)(key, ctx, conf)
    }
}

inventory::collect!(StrategyPlugin);

pub type StrategyPluginRegistry<'a> = HashMap<&'a str, &'a StrategyPlugin>;

pub fn gather_plugins() -> StrategyPluginRegistry<'static> {
    inventory::iter::<StrategyPlugin>
        .into_iter()
        .map(|p| (p.name, p))
        .collect()
}

lazy_static! {
    static ref PLUGIN_REGISTRY: StrategyPluginRegistry<'static> = { gather_plugins() };
}

pub fn plugin_registry() -> &'static StrategyPluginRegistry<'static> { &PLUGIN_REGISTRY }
