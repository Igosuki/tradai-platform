/*!
Defines a backtesting tool for all strategies available through `StrategyPlugin`

# Overview

A backtest spawns all configured strategies, feeds them market events from available datasources, and
outputs the state of each strategy to reports after every iteration.
Additionally, the strategies are ranked by PnL in a global report.

If using persistent storage, the databases can be re-used by a trading server directly to run a strategy
with the same configuration as the backtest.

 */

#![allow(
    clippy::wildcard_imports,
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::unused_self,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
#![feature(box_patterns)]
#![feature(map_try_insert)]
#![feature(path_try_exists)]
#![feature(exact_size_is_empty)]
#![feature(associated_type_defaults)]
// TODO: https://github.com/rust-lang/rust/issues/47384
#![allow(clippy::single_component_path_imports)]

#[macro_use]
extern crate async_stream;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate tracing;
#[macro_use]
extern crate anyhow;
extern crate core;
#[macro_use]
extern crate strum_macros;

// // TODO: https://github.com/rust-lang/rust/issues/47384
// use brokers::prelude::*;
// use strategy::prelude::*;
// // TODO: https://github.com/rust-lang/rust/issues/47384

mod backtest;
mod config;
mod datafusion_util;
mod dataset;
mod datasources;
mod error;
pub mod report;
mod runner;

pub use crate::{backtest::*,
                config::*,
                dataset::{DataFormat, DatasetCatalog, DatasetReader, MarketEventDatasetType},
                error::*};
pub use datafusion::record_batch::RecordBatch;
