/*!
A collection of statistical tools related to trading

# Overview

Indicators : re-exports of the `ta` library plus some more technical indicators
Math : optimal algorithms for common math functions
Summary : statistical tools to summarize data series

 */

#![feature(test)]
#![feature(type_alias_impl_trait)]
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
extern crate serde_derive;
#[cfg(test)]
extern crate test;
#[macro_use]
extern crate thiserror;
#[macro_use]
extern crate anyhow;

pub use ta::indicators::*;
pub use ta::{Close, High, Low, Next, Open, Period, Reset, Volume};
pub use yata::core::{Action, Method, Window};
pub use yata::methods::{LowerReversalSignal, UpperReversalSignal};

mod dispersion;
mod error;
pub mod indicators;
pub mod iter;
mod math;
mod summary;
