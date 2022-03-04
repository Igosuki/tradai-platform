/*!
A collection of statistical tools related to trading

# Overview

Indicators : re-exports of the `ta` library plus some more technical indicators
Math : optimal algorithms for common math functions
Summary : statistical tools to summarize data series

 */

#![feature(test)]
#![feature(type_alias_impl_trait)]
#![feature(generic_associated_types)]
#![allow(
    clippy::items_after_statements,
    clippy::unreadable_literal,
    clippy::wildcard_imports,
    clippy::must_use_candidate,
    clippy::module_name_repetitions
)]

#[cfg(test)]
#[macro_use]
extern crate float_cmp;
#[macro_use]
extern crate prettytable;
#[macro_use]
extern crate serde;
#[cfg(test)]
extern crate test;
#[macro_use]
extern crate thiserror;
#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate strum_macros;

pub use ta::indicators as ta_indicators;
pub use ta::{Close, High, Low, Next, Open, Period, Reset, Volume};
pub use yata::core::{Action, Method, Source, Window};
pub use yata::helpers::MA;
pub use yata::indicators as yata_indicators;
pub use yata::methods::*;
pub use yata::prelude as yata_prelude;

pub mod dispersion;
pub mod error;
pub mod indicators;
pub mod iter;
pub mod kline;
pub mod math;
pub mod summary;
