#![feature(generic_associated_types)]
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate derivative;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate tracing;

use strategy::plugin::{provide_options, StrategyPlugin};

pub mod mean_reverting;
pub mod naive_pair_trading;
// TODO: this should not be required to be at the crate root, cf : https://github.com/dtolnay/inventory/issues/9
inventory::submit! {
    StrategyPlugin::new("mean_reverting", provide_options::<crate::mean_reverting::options::Options>, crate::mean_reverting::provide_strat)
}
