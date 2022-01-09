/*!
Defines common standard functionality used by other crates.

# Overview

Utility functions are typically formatting, logging, timing, tracing and ser/deserializing

 */

#![allow(
    // noisy
    clippy::missing_errors_doc,
    clippy::module_name_repetitions,
    clippy::implicit_hasher
)]

#[cfg(test)]
#[macro_use]
extern crate async_stream;
#[macro_use]
extern crate maplit;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate tokio;
#[macro_use]
extern crate tracing;

pub mod compress;
pub mod log;
pub mod s3;
pub mod ser;
pub mod test;
pub mod time;
pub mod trace;
