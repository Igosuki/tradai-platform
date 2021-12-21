#[macro_use]
extern crate maplit;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate tracing;

use std::path::Path;
use std::sync::Arc;

use db::{get_or_create, DbOptions, Storage};

pub mod draw;
pub mod fs;
pub mod input;
pub mod it_backtest;
pub mod log;

pub fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

pub fn test_db() -> Arc<dyn Storage> {
    let path = util::test::test_dir();
    get_or_create(&DbOptions::new(path), "", vec![])
}

pub fn test_db_with_path<S: AsRef<Path>>(path: S) -> Arc<dyn Storage> {
    let options = DbOptions::new(path);
    get_or_create(&options, "", vec![])
}
