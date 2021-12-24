#![feature(test)]
#![feature(type_alias_impl_trait)]

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

pub use ta::indicators::*;
pub use ta::*;

mod dispersion;
mod error;
pub mod indicators;
pub mod iter;
mod math;
mod summary;
