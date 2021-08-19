#![feature(test)]

#[macro_use]
extern crate thiserror;
#[macro_use]
extern crate serde_derive;

#[cfg(test)]
extern crate test;
#[cfg(test)]
#[macro_use]
extern crate float_cmp;

pub use ta::*;

mod error;
pub mod indicators;
pub mod iter;
