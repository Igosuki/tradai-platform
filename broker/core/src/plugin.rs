use once_cell::sync::OnceCell;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::api::Brokerage;
use crate::bot::{BrokerageAccountDataStreamer, MarketDataStreamer};
use crate::credential::Credentials;
use crate::error::Result;
use crate::exchange::Exchange;
use crate::settings::BrokerSettings;
use crate::types::{AccountType, MarketSymbol, PrivateStreamChannel, StreamChannel};

#[async_trait(?Send)]
pub trait BrokerConnector: Send + Sync {
    async fn new_api(&self, ctx: BrokerageInitContext) -> Result<Arc<dyn Brokerage>>;

    async fn new_public_stream(&self, ctx: BrokerageBotInitContext) -> Result<Box<MarketDataStreamer>>;

    async fn new_private_stream(&self, ctx: PrivateBotInitContext) -> Result<Box<BrokerageAccountDataStreamer>>;
}

#[derive(typed_builder::TypedBuilder)]
pub struct BrokerageInitContext {
    pub use_test_servers: bool,
    pub creds: Box<dyn Credentials>,
}

impl BrokerageInitContext {
    pub fn new(use_test_servers: bool, creds: Box<dyn Credentials>) -> Self {
        BrokerageInitContext::builder()
            .use_test_servers(use_test_servers)
            .creds(creds)
            .build()
    }
}

#[derive(typed_builder::TypedBuilder)]
pub struct BrokerageBotInitContext {
    pub settings: BrokerSettings,
    pub creds: Box<dyn Credentials>,
    pub channels: HashMap<StreamChannel, HashSet<MarketSymbol>>,
}

#[derive(typed_builder::TypedBuilder)]
pub struct PrivateBotInitContext {
    pub use_test: bool,
    pub creds: Box<dyn Credentials>,
    pub channels: HashSet<PrivateStreamChannel>,
    pub account_type: AccountType,
}

pub struct BrokerPlugin {
    name: Exchange,
    connector: fn() -> Box<dyn BrokerConnector>,
}

impl Debug for BrokerPlugin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerPlugin").field("name", &self.name).finish()
    }
}

impl BrokerPlugin {
    pub const fn new(name: Exchange, connector: fn() -> Box<dyn BrokerConnector>) -> Self { Self { name, connector } }

    pub async fn new_api(&self, creds: Box<dyn Credentials>, use_test_servers: bool) -> Result<Arc<dyn Brokerage>> {
        let ctx = BrokerageInitContext::new(use_test_servers, creds);
        (self.connector)().new_api(ctx).await
    }

    pub async fn new_public_stream<'a>(&self, ctx: BrokerageBotInitContext) -> Result<Box<MarketDataStreamer>> {
        (self.connector)().new_public_stream(ctx).await
    }

    pub async fn new_private_stream<'a>(
        &self,
        ctx: PrivateBotInitContext,
    ) -> Result<Box<BrokerageAccountDataStreamer>> {
        (self.connector)().new_private_stream(ctx).await
    }
}

inventory::collect!(BrokerPlugin);

pub type BrokerPluginRegistry<'a> = HashMap<Exchange, &'a BrokerPlugin>;

pub fn gather_plugins() -> BrokerPluginRegistry<'static> {
    inventory::iter::<BrokerPlugin>
        .into_iter()
        .map(|p| (p.name, p))
        .collect()
}

static ECHANGE_PLUGIN_REGISTRY: OnceCell<BrokerPluginRegistry<'static>> = OnceCell::new();

pub fn plugin_registry() -> &'static BrokerPluginRegistry<'static> {
    ECHANGE_PLUGIN_REGISTRY.get_or_init(gather_plugins)
}

pub fn get_exchange_plugin(exchange: Exchange) -> crate::error::Result<&'static BrokerPlugin> {
    crate::plugin::plugin_registry()
        .get(&exchange)
        .copied()
        .ok_or_else(|| crate::error::Error::InvalidExchange(exchange.to_string()))
}

mod macros {
    #[macro_export]
    macro_rules! exchange {
        ($f:expr, $t:ident) => {
            pub struct $t;
            pub fn provide_connector() -> Box<dyn $crate::plugin::BrokerConnector> { Box::new($t {}) }

            $crate::inventory::submit! {
                $crate::plugin::BrokerPlugin::new($f, provide_connector)
            }
        };
    }
}
