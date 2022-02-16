/*!
A collection of pre-developped strategies

# Overview

Mean Reverting : a MACD variant to enter a position if the oscillator goes over a threshold
Naive Spread : a linear regression that enters a position depending on the direction of spread between two markets

# Nota Bene

Python equivalents can be found in `py/`

 */

#![feature(used_with_arg)]
#![feature(generic_associated_types)]
#![allow(
    clippy::wildcard_imports,
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::missing_errors_doc
)]

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

pub mod bbplusb;
pub mod breakout;
pub mod mean_reverting;
pub mod naive_pair_trading;
