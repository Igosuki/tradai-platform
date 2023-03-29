//! Use this module to create a generic API.

use std::collections::HashSet;
use std::fmt::Debug;
use std::path::PathBuf;

use serde_json::Value;

use crate::api::Brokerage;
use crate::bot::{BrokerageAccountDataStreamer, MarketDataStreamer};
use crate::credential::{BasicCredentials, Credentials};
use crate::error::{Error, Result};
use crate::exchange::Exchange;
use crate::manager::{BrokerageManager, BrokerageRegistry};
use crate::pair::refresh_pairs;
use crate::plugin::{get_exchange_plugin, BrokerageBotInitContext, PrivateBotInitContext};
use crate::settings::BrokerSettings;
use crate::types::{AccountType, MarketChannel, PrivateStreamChannel};

#[derive(Debug)]
pub struct Brokerages;

impl Brokerages {
    #[must_use]
    pub fn new_manager() -> BrokerageManager { BrokerageManager::new() }

    pub async fn public_apis(echanges: &[Exchange]) -> BrokerageRegistry {
        let exchange_apis: BrokerageRegistry = BrokerageRegistry::new();
        let manager = Brokerages::new_manager();
        for xch in echanges {
            let api = manager.build_public_exchange_api(xch, false).await.unwrap();
            exchange_apis.insert(*xch, api);
            manager.new_fee_provider(*xch, Value::Null).unwrap();
        }
        exchange_apis
    }

    /// # Panics
    ///
    /// will panic if no streams are implemented for the exchange
    ///
    /// # Errors
    ///
    /// If the exchange bot fails to connect
    pub async fn new_market_bot<'a>(
        exchange: Exchange,
        creds: Box<dyn Credentials>,
        s: BrokerSettings,
        market_channels: &[MarketChannel],
    ) -> Result<Box<MarketDataStreamer>> {
        let plugin = get_exchange_plugin(exchange)?;
        let ctx = BrokerageBotInitContext::builder()
            .settings(s.clone())
            .creds(creds)
            .channels(market_channels.to_vec())
            .build();
        plugin.new_public_stream(ctx).await
    }

    /// # Panics
    ///
    /// will panic if no streams are implemented for the exchange
    ///
    /// # Errors
    ///
    /// If the exchange bot fails to connect
    #[allow(clippy::single_match_else)]
    pub async fn new_account_stream(
        exchange: Exchange,
        creds: Box<dyn Credentials>,
        use_test: bool,
        account_type: AccountType,
        channels: HashSet<PrivateStreamChannel>,
    ) -> Result<Box<BrokerageAccountDataStreamer>> {
        let plugin = get_exchange_plugin(exchange)?;
        let ctx = PrivateBotInitContext::builder()
            .use_test(use_test)
            .creds(creds)
            .channels(channels)
            .account_type(account_type)
            .build();
        plugin.new_private_stream(ctx).await
    }

    /// # Panics
    ///
    /// will panic if no key is found in the keys file for the exchange
    ///
    /// # Errors
    ///
    /// If the credentials are missing
    pub fn credentials_for(exchange: Exchange, path: PathBuf) -> Result<Box<dyn Credentials>> {
        let all_creds = BasicCredentials::new_from_file(path)?;

        let account_key = match exchange {
            Exchange::Bitstamp => "account_bitstamp",
            Exchange::Bittrex => "account_bittrex",
            Exchange::Binance => "account_binance",
            _ => panic!(),
        };
        all_creds
            .get(account_key)
            .map(|b| dyn_clone::clone_box(b.as_ref()))
            .ok_or_else(|| Error::MissingCredentials(account_key.to_string()))
    }

    /// # Errors
    ///
    /// If the registry fails to refresh
    pub async fn load_pair_registry(xch: &Exchange, api: &'_ dyn Brokerage) -> Result<()> {
        refresh_pairs(xch, api).await
    }

    /// # Errors
    ///
    /// If the registries fails to load pairs
    pub async fn load_pair_registries(apis: &BrokerageRegistry) -> Result<()> {
        futures::future::join_all(apis.clone().iter().map(|entry| async move {
            let arc = entry.value().clone();
            Self::load_pair_registry(entry.key(), arc.as_ref()).await
        }))
        .await
        .into_iter()
        .collect()
    }
}
