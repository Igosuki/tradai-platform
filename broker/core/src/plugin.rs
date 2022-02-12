use once_cell::sync::OnceCell;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::api::ExchangeApi;
use crate::bot::{AccountExchangeBot, MarketExchangeBot};
use crate::credential::Credentials;
use crate::error::Result;
use crate::exchange::Exchange;
use crate::settings::ExchangeSettings;
use crate::types::{AccountType, PrivateStreamChannel, StreamChannel, Symbol};

#[async_trait(?Send)]
pub trait ExchangeConnector: Send + Sync {
    async fn new_api(&self, ctx: ExchangeApiInitContext) -> Result<Arc<dyn ExchangeApi>>;

    async fn new_public_stream(&self, ctx: ExchangeBotInitContext) -> Result<Box<MarketExchangeBot>>;

    async fn new_private_stream(&self, ctx: PrivateBotInitContext) -> Result<Box<AccountExchangeBot>>;
}

#[derive(typed_builder::TypedBuilder)]
pub struct ExchangeApiInitContext {
    pub use_test_servers: bool,
    pub creds: Box<dyn Credentials>,
}

impl ExchangeApiInitContext {
    pub fn new(use_test_servers: bool, creds: Box<dyn Credentials>) -> Self {
        ExchangeApiInitContext::builder()
            .use_test_servers(use_test_servers)
            .creds(creds)
            .build()
    }
}

#[derive(typed_builder::TypedBuilder)]
pub struct ExchangeBotInitContext {
    pub settings: ExchangeSettings,
    pub creds: Box<dyn Credentials>,
    pub channels: HashMap<StreamChannel, HashSet<Symbol>>,
}

#[derive(typed_builder::TypedBuilder)]
pub struct PrivateBotInitContext {
    pub use_test: bool,
    pub creds: Box<dyn Credentials>,
    pub channels: HashSet<PrivateStreamChannel>,
    pub account_type: AccountType,
}

pub struct ExchangePlugin {
    name: Exchange,
    connector: fn() -> Box<dyn ExchangeConnector>,
}

impl Debug for ExchangePlugin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExchangePlugin").field("name", &self.name).finish()
    }
}

impl ExchangePlugin {
    pub const fn new(name: Exchange, connector: fn() -> Box<dyn ExchangeConnector>) -> Self { Self { name, connector } }

    pub async fn new_api(&self, creds: Box<dyn Credentials>, use_test_servers: bool) -> Result<Arc<dyn ExchangeApi>> {
        let ctx = ExchangeApiInitContext::new(use_test_servers, creds);
        (self.connector)().new_api(ctx).await
    }

    pub async fn new_public_stream<'a>(&self, ctx: ExchangeBotInitContext) -> Result<Box<MarketExchangeBot>> {
        (self.connector)().new_public_stream(ctx).await
    }

    pub async fn new_private_stream<'a>(&self, ctx: PrivateBotInitContext) -> Result<Box<AccountExchangeBot>> {
        (self.connector)().new_private_stream(ctx).await
    }
}

inventory::collect!(ExchangePlugin);

pub type ExchangePluginRegistry<'a> = HashMap<Exchange, &'a ExchangePlugin>;

pub fn gather_plugins() -> ExchangePluginRegistry<'static> {
    inventory::iter::<ExchangePlugin>
        .into_iter()
        .map(|p| (p.name, p))
        .collect()
}

static ECHANGE_PLUGIN_REGISTRY: OnceCell<ExchangePluginRegistry<'static>> = OnceCell::new();

pub fn plugin_registry() -> &'static ExchangePluginRegistry<'static> {
    ECHANGE_PLUGIN_REGISTRY.get_or_init(gather_plugins)
}

pub fn get_exchange_plugin(exchange: Exchange) -> crate::error::Result<&'static ExchangePlugin> {
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
            fn provide_connector() -> Box<dyn $crate::plugin::ExchangeConnector> { Box::new($t {}) }

            $crate::inventory::submit! {
                $crate::plugin::ExchangePlugin::new($f, provide_connector)
            }
        };
    }
}
