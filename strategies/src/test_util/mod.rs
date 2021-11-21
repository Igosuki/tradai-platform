use std::sync::Arc;

use db::{get_or_create, DbOptions, Storage};

#[cfg(test)]
pub mod draw;
#[cfg(test)]
pub mod fs;
#[cfg(test)]
pub mod input;
#[cfg(test)]
pub mod log;

pub fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

pub fn test_db() -> Arc<dyn Storage> {
    let path = util::test::test_dir();
    get_or_create(&DbOptions::new(path), "", vec![])
}
