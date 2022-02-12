#[cfg(any(feature = "binance", feature = "all_exchanges"))]
pub use broker_binance;
#[cfg(any(feature = "bitstamp", feature = "all_exchanges"))]
pub use broker_bitstamp;
#[cfg(any(feature = "bittrex", feature = "all_exchanges"))]
pub use broker_bittrex;
#[cfg(any(feature = "coinbase", feature = "all_exchanges"))]
pub use broker_coinbase;
#[cfg(any(feature = "kraken", feature = "all_exchanges"))]
pub use broker_kraken;
#[cfg(any(feature = "poloniex", feature = "all_exchanges"))]
pub use broker_poloniex;

pub mod prelude {
    pub use broker_core::brokerages::Brokerages;
    pub use broker_core::prelude::*;
}

pub use broker_core::brokerages::Brokerages;
pub use broker_core::*;
