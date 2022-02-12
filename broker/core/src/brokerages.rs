//! Use this module to create a generic API.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;

use crate::api::ExchangeApi;
use crate::bot::{AccountExchangeBot, MarketExchangeBot};
use crate::credential::{BasicCredentials, Credentials};
use crate::error::{Error, Result};
use crate::exchange::Exchange;
use crate::manager::{ExchangeApiRegistry, ExchangeManager};
use crate::pair::{filter_pairs, refresh_pairs};
use crate::plugin::{get_exchange_plugin, ExchangeBotInitContext, PrivateBotInitContext};
use crate::settings::{ExchangeSettings, OrderbookSettings, OrderbookStyle, TradesSettings};
use crate::types::{AccountType, Pair, PrivateStreamChannel, StreamChannel, Symbol};

#[derive(Debug)]
pub struct Brokerages;

impl Brokerages {
    #[must_use]
    pub fn new_manager() -> ExchangeManager { ExchangeManager::new() }

    pub async fn public_apis(echanges: &[Exchange]) -> ExchangeApiRegistry {
        let exchange_apis: DashMap<Exchange, Arc<dyn ExchangeApi>> = DashMap::new();
        let manager = Brokerages::new_manager();
        for xch in echanges {
            let api = manager.build_public_exchange_api(xch, false).await.unwrap();
            exchange_apis.insert(*xch, api);
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
        s: ExchangeSettings,
    ) -> Result<Box<MarketExchangeBot>> {
        let mut channels: HashMap<StreamChannel, HashSet<Symbol>> = HashMap::new();
        if let Some(OrderbookSettings { ref symbols, ref style }) = s.orderbook {
            let pairs = filter_pairs(&exchange, symbols)?;
            let order_book_pairs: HashSet<Pair> = pairs
                .into_iter()
                .filter(|p| crate::pair::pair_to_symbol(&exchange, &p.clone()).is_ok())
                .collect();
            // Live order book pairs
            let channel = match style {
                OrderbookStyle::Live => StreamChannel::PlainOrderbook,
                OrderbookStyle::Detailed => StreamChannel::DetailedOrderbook,
                OrderbookStyle::Diff => StreamChannel::DiffOrderbook,
            };
            channels.insert(channel, order_book_pairs);
        }
        if let Some(TradesSettings { ref symbols }) = s.trades {
            // Live trade pairs
            let pairs = filter_pairs(&exchange, symbols)?;
            let trade_pairs: HashSet<Symbol> = pairs
                .into_iter()
                .filter(|p| crate::pair::pair_to_symbol(&exchange, p).is_ok())
                .collect();
            channels.insert(StreamChannel::Trades, trade_pairs);
        }
        debug!("{:?}", channels);
        let plugin = get_exchange_plugin(exchange)?;
        let ctx = ExchangeBotInitContext::builder()
            .settings(s.clone())
            .creds(creds)
            .channels(channels)
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
    ) -> Result<Box<AccountExchangeBot>> {
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
    pub async fn load_pair_registry(xch: &Exchange, api: &'_ dyn ExchangeApi) -> Result<()> {
        refresh_pairs(xch, api).await
    }

    /// # Errors
    ///
    /// If the registries fails to load pairs
    pub async fn load_pair_registries(apis: &DashMap<Exchange, Arc<dyn ExchangeApi>>) -> Result<()> {
        futures::future::join_all(apis.clone().iter().map(|entry| async move {
            let arc = entry.value().clone();
            Self::load_pair_registry(entry.key(), arc.as_ref()).await
        }))
        .await
        .into_iter()
        .collect()
    }
}
