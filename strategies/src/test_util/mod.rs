use std::sync::Arc;

use db::{get_or_create, DbOptions, Storage};

pub use self::binance::account_ws as binance_account_ws;
pub use self::http::ws_it_server;

#[cfg(test)]
pub mod binance;
#[cfg(test)]
pub mod draw;
#[cfg(any(feature = "live_e2e_tests", feature = "manual_e2e_tests"))]
#[cfg(test)]
pub mod e2e;
#[cfg(test)]
pub mod fs;
#[cfg(test)]
pub mod http;
#[cfg(test)]
pub mod input;
#[cfg(test)]
pub mod log;

pub fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

pub fn test_db() -> Arc<dyn Storage> {
    let path = util::test::test_dir();
    get_or_create(&DbOptions::new(path), "", vec![])
}
