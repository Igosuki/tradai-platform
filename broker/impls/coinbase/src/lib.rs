//! Use this module to interact with Bitstamp exchange.

#[macro_use]
extern crate broker_core;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate anyhow;

use broker_core::prelude::*;
use std::sync::Arc;

mod api;
mod generic_api;
mod utils;

pub use self::api::CoinbaseApi;

#[async_trait(?Send)]
impl ExchangeConnector for CoinbaseExchangeConnector {
    async fn new_api(&self, ctx: ExchangeApiInitContext) -> broker_core::error::Result<Arc<dyn ExchangeApi>> {
        let api = CoinbaseApi::new(ctx.creds.as_ref())?;
        Ok(Arc::new(api))
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

exchange!(Exchange::Coinbase, CoinbaseExchangeConnector);
