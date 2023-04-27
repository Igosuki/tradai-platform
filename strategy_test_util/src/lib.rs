/*!
Test utils for `strategy` isolated in a crate so as not to pollute the crate with feature toggles

 */

#![allow(
    clippy::missing_errors_doc,
    clippy::module_name_repetitions,
    clippy::unused_async,
    clippy::must_use_candidate,
    clippy::wildcard_imports
)]

#[macro_use]
extern crate maplit;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate tracing;
extern crate core;

use std::path::Path;
use std::sync::Arc;

use db::{get_or_create, DbOptions, Storage};

pub mod draw;
pub mod fs;
pub mod input;
pub mod it_backtest;
pub mod log;
pub mod plugin;

pub fn test_db() -> Arc<dyn Storage> {
    let path = util::test::test_dir();
    get_or_create(&DbOptions::new(path), "", vec![])
}

pub fn test_db_with_path<S: AsRef<Path>>(path: S) -> Arc<dyn Storage> {
    let options = DbOptions::new(path);
    get_or_create(&options, "", vec![])
}
