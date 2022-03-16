use dashmap::DashMap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::api::{Brokerage, MockBrokerage};
use crate::brokerages::Brokerages;
use crate::credential::{BasicCredentials, Credentials};
use crate::error::Result;
use crate::exchange::Exchange;
use crate::fees::FeeProvider;
use crate::plugin::get_exchange_plugin;
use crate::settings::BrokerSettings;
use crate::types::{AssetType, OrderType};

pub type BrokerageRegistry = DashMap<Exchange, Arc<dyn Brokerage>>;
pub type FeesProviderRegistry = DashMap<Exchange, Arc<dyn FeeProvider>>;

pub type BrokerageManagerRef = Arc<BrokerageManager>;

#[derive(Default, Debug, Clone)]
pub struct BrokerageManager {
    exchange_apis: BrokerageRegistry,
    fees_providers: FeesProviderRegistry,
}

impl BrokerageManager {
    #[must_use]
    pub fn new() -> Self { Self::default() }

    #[must_use]
    pub fn new_with_reg(exchange_apis: DashMap<Exchange, Arc<dyn Brokerage>>) -> Self {
        Self {
            exchange_apis,
            fees_providers: Default::default(),
        }
    }

    #[must_use]
    pub fn get_api(&self, xchg: Exchange) -> Option<Arc<dyn Brokerage>> {
        self.exchange_apis.get(&xchg).map(|v| v.value().clone())
    }

    #[must_use]
    pub fn expect_api(&self, xchg: Exchange) -> Arc<dyn Brokerage> {
        self.exchange_apis.get(&xchg).map(|v| v.value().clone()).unwrap()
    }

    pub fn remove_api(&mut self, xchg: Exchange) -> Option<Arc<dyn Brokerage>> {
        self.exchange_apis.remove(&xchg).map(|v| v.1)
    }

    #[must_use]
    pub fn exchange_apis(&self) -> &BrokerageRegistry { &self.exchange_apis }

    /// # Panics
    ///
    /// if any of the exchange apis cannot be built
    pub async fn build_exchange_apis(&self, exchanges: Arc<HashMap<Exchange, BrokerSettings>>, keys_path: PathBuf) {
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
            self.exchange_apis.insert(*xch, Arc::new(MockBrokerage::default()));
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
    ) -> Result<Arc<dyn Brokerage>> {
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
    ) -> Result<Arc<dyn Brokerage>> {
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
    ) -> Result<Arc<dyn Brokerage>> {
        let plugin = get_exchange_plugin(exchange)?;
        plugin.new_api(creds, use_test_servers).await
    }

    pub fn new_fee_provider(&self, exchange: Exchange, conf: serde_json::Value) -> Result<()> {
        let plugin = get_exchange_plugin(exchange)?;
        self.fees_providers.insert(exchange, plugin.new_fees_provider(conf)?);
        Ok(())
    }

    pub fn get_fees_rate(
        &self,
        exchange: Exchange,
        asset_type: Option<AssetType>,
        order_type: Option<OrderType>,
    ) -> Option<f64> {
        self.fees_providers
            .get(&exchange)
            .map(|v| v.value().get_rate(asset_type, order_type).0)
    }
}
