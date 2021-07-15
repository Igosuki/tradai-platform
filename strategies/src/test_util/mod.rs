#[cfg(test)]
pub mod binance;
#[cfg(test)]
pub mod http;

pub fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

pub use self::binance::{account_ws as binance_account_ws, local_api};
pub use self::http::ws_it_server;
