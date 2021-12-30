#![feature(assert_matches)]
#![allow(
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::wildcard_imports,
    clippy::missing_errors_doc,
    clippy::unused_async
)]

#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate tracing;
#[macro_use]
extern crate async_trait;
#[macro_use]
#[cfg(test)]
extern crate float_cmp;

pub mod balance;
mod error;
pub mod margin;
pub mod portfolio;
pub mod risk;

pub use error::*;

#[cfg(test)]
mod test_util;
