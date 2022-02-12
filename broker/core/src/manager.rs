use dashmap::DashMap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::api::{ExchangeApi, MockExchangeApi};
use crate::brokerages::Brokerages;
use crate::credential::{BasicCredentials, Credentials};
use crate::error::Result;
use crate::exchange::Exchange;
use crate::plugin::get_exchange_plugin;
use crate::settings::ExchangeSettings;

pub type ExchangeApiRegistry = DashMap<Exchange, Arc<dyn ExchangeApi>>;

pub type ExchangeManagerRef = Arc<ExchangeManager>;

#[derive(Default, Debug, Clone)]
pub struct ExchangeManager {
    exchange_apis: ExchangeApiRegistry,
}

impl ExchangeManager {
    #[must_use]
    pub fn new() -> Self { Self::default() }

    #[must_use]
    pub fn new_with_reg(exchange_apis: DashMap<Exchange, Arc<dyn ExchangeApi>>) -> Self { Self { exchange_apis } }

    #[must_use]
    pub fn get_api(&self, xchg: Exchange) -> Option<Arc<dyn ExchangeApi>> {
        self.exchange_apis.get(&xchg).map(|v| v.value().clone())
    }

    #[must_use]
    pub fn expect_api(&self, xchg: Exchange) -> Arc<dyn ExchangeApi> {
        self.exchange_apis.get(&xchg).map(|v| v.value().clone()).unwrap()
    }

    pub fn remove_api(&mut self, xchg: Exchange) -> Option<Arc<dyn ExchangeApi>> {
        self.exchange_apis.remove(&xchg).map(|v| v.1)
    }

    #[must_use]
    pub fn exchange_apis(&self) -> &ExchangeApiRegistry { &self.exchange_apis }

    /// # Panics
    ///
    /// if any of the exchange apis cannot be built
    pub async fn build_exchange_apis(&self, exchanges: Arc<HashMap<Exchange, ExchangeSettings>>, keys_path: PathBuf) {
        for (xch, conf) in exchanges.iter() {
            let xch_api = self
                .build_exchange_api(keys_path.clone(), xch, conf.use_test)
                .await
                .unwrap();
            self.exchange_apis.insert(*xch, xch_api);
        }
    }

    pub fn build_mock_exchange_apis(&self, exchanges: &[Exchange]) {
        for xch in exchanges.iter() {
            self.exchange_apis.insert(*xch, Arc::new(MockExchangeApi::default()));
        }
    }

    /// # Errors
    ///
    /// if credentials cannot be acquired or the api is not properly configured
    pub async fn build_exchange_api(
        &self,
        keys_path: PathBuf,
        xch: &Exchange,
        use_test_servers: bool,
    ) -> Result<Arc<dyn ExchangeApi>> {
        let creds = Brokerages::credentials_for(*xch, keys_path)?;
        self.new_exchange_with_options(*xch, creds, use_test_servers).await
    }

    /// # Errors
    ///
    /// if no implemented api can be configured for the exchange
    pub async fn build_public_exchange_api(
        &self,
        xch: &Exchange,
        use_test_servers: bool,
    ) -> Result<Arc<dyn ExchangeApi>> {
        let credentials = BasicCredentials::empty(*xch);
        self.new_exchange_with_options(*xch, Box::new(credentials), use_test_servers)
            .await
    }

    /// # Panics
    ///
    /// If no api is implemented for the exchange
    ///
    /// # Errors
    ///
    /// IF the api fails to initialize with the provided credentials
    pub async fn new_exchange_with_options(
        &self,
        exchange: Exchange,
        creds: Box<dyn Credentials>,
        use_test_servers: bool,
    ) -> Result<Arc<dyn ExchangeApi>> {
        let plugin = get_exchange_plugin(exchange)?;
        plugin.new_api(creds, use_test_servers).await
    }
}
