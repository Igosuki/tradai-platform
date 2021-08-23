#[cfg(test)]
pub mod binance;
#[cfg(test)]
pub mod http;
#[cfg(test)]
pub mod log;

use db::{get_or_create, DbOptions, Storage};
use std::sync::Arc;

pub fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

pub fn test_results_dir(modle_path: &str) -> String {
    let module_path = modle_path.replace("::", "_");
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let test_results_dir = format!("{}/target/test_results/{}", manifest_dir, module_path);
    std::fs::create_dir_all(&test_results_dir).unwrap();
    test_results_dir
}

pub fn test_db() -> Arc<dyn Storage> {
    let path = util::test::test_dir();
    get_or_create(&DbOptions::new(path), "", vec![])
}

pub use self::binance::account_ws as binance_account_ws;
pub use self::http::ws_it_server;
