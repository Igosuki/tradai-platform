//! Use this module to interact with Kraken exchange.
//! See examples for more informations.

#[macro_use]
extern crate broker_core;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate serde;

use std::sync::Arc;

use broker_core::prelude::*;

mod api;
mod generic_api;
mod model;
mod utils;

pub use self::api::KrakenApi;

#[async_trait(?Send)]
impl ExchangeConnector for KrakenExchangeConnector {
    async fn new_api(&self, ctx: ExchangeApiInitContext) -> broker_core::error::Result<Arc<dyn ExchangeApi>> {
        Ok(Arc::new(KrakenApi::new(ctx.creds.as_ref())?))
    }

    async fn new_public_stream(
        &self,
        _ctx: ExchangeBotInitContext,
    ) -> broker_core::error::Result<Box<MarketExchangeBot>> {
        todo!()
    }

    async fn new_private_stream(
        &self,
        _ctx: PrivateBotInitContext,
    ) -> broker_core::error::Result<Box<AccountExchangeBot>> {
        todo!()
    }
}

exchange!(Exchange::Kraken, KrakenExchangeConnector);
