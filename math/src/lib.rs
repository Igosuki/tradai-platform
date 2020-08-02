#![feature(iterator_fold_self)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate thiserror;
#[macro_use]
extern crate serde_derive;

mod error;
pub mod iter;
pub mod moving_average;
mod traits;
pub use traits::*;
