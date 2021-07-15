#![feature(test)]

extern crate log;
#[macro_use]
extern crate thiserror;
#[macro_use]
extern crate serde_derive;

#[cfg(test)]
extern crate test;
#[cfg(test)]
#[macro_use]
extern crate float_cmp;

mod error;
pub mod iter;
pub mod moving_average;
mod traits;
pub use traits::*;
