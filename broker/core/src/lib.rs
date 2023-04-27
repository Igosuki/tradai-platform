//! ![Brokers](https://raw.githubusercontent.com/Igosuki/tradai-platform/master/tradai.png)
//!
//! The brokers library provides interfaces to be implemented to provide additional implementations
//! of building block of the Tradai platform.
//! ### Interfaces
//! - Broker api : fetch tickers, exchange configuration, manage orders, query private information
//! - Broker streaming api : streaming public and private data
//! - Pair registry : unified registry of currency / pairs
//!
//! ### Exchanges support:
//! - [x] Poloniex
//! - [x] Kraken
//! - [x] Bitstamp
//! - [x] Bittrex
//! - [x] Gdax
//! - [x] Binance
//!
//! ### N.B.:
//! - The library expects pair configurations to be loaded in the registry before doing any trading, see PairRegistry

#![deny(unused_must_use, unused_mut)]
// error_chain can make a lot of recursions.
#![recursion_limit = "256"]
// Allow lint customization.
#![allow(unknown_lints)]
// Avoid warning for the Crypto-currency about quotes.
#![allow(
    unused_braces,
    clippy::doc_markdown,
    clippy::similar_names,
    clippy::boxed_local,
    clippy::redundant_else,
    clippy::wildcard_imports,
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::must_use_candidate
)]
#![feature(async_closure)]
#![feature(try_trait_v2)]
#![feature(test)]
#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]

#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate actix_derive;
#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate strum_macros;
#[cfg(test)]
extern crate test;
#[macro_use]
extern crate tracing;

#[cfg(not(any(feature = "rustls-tls", feature = "native-tls")))]
compile_error!("Pick at least one secure ssl implementation between rustls-tls and native-tls");

pub mod account_metrics;
pub mod api;
pub mod bot;
pub mod broker;
pub mod brokerages;
pub mod credential;
pub mod currency;
pub mod error;
pub mod exchange;
pub mod fees;
pub mod json_util;
pub mod manager;
pub mod margin_interest_rates;
pub mod metrics;
pub mod metrics_util;
pub mod pair;
pub mod plugin;
pub mod settings;
pub mod streaming_api;
pub mod types;
pub mod url_util;

pub use inventory;

pub mod prelude {
    #[doc(no_inline)]
    pub use crate::api::Brokerage;
    #[doc(no_inline)]
    pub use crate::bot::{BrokerageAccountDataStreamer, MarketDataStreamer};
    #[doc(no_inline)]
    pub use crate::credential::Credentials;
    #[doc(no_inline)]
    pub use crate::exchange::Exchange;
    #[doc(no_inline)]
    pub use crate::manager::*;
    #[doc(no_inline)]
    pub use crate::plugin::{BrokerConnector, BrokerageBotInitContext, BrokerageInitContext, PrivateBotInitContext};
    #[doc(no_inline)]
    pub use crate::settings::BrokerSettings;
    #[doc(no_inline)]
    pub use crate::types::{AccountEvent, AccountEventEnveloppe, AccountType, AddOrderRequest, Asset, AssetType,
                           MarketEvent, MarketEventEnvelope, OrderEnforcement, OrderQuery, OrderType, Orderbook, Pair,
                           TradeType};
    pub use util::time::{get_unix_timestamp_ms, get_unix_timestamp_us};
}
