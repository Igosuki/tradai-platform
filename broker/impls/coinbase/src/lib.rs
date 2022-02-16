//! Use this module to interact with Bitstamp exchange.

#![feature(used_with_arg)]

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
impl BrokerConnector for CoinbaseExchangeConnector {
    async fn new_api(&self, ctx: BrokerageInitContext) -> broker_core::error::Result<Arc<dyn Brokerage>> {
        let api = CoinbaseApi::new(ctx.creds.as_ref())?;
        Ok(Arc::new(api))
    }

    async fn new_public_stream(
        &self,
        _ctx: BrokerageBotInitContext,
    ) -> broker_core::error::Result<Box<MarketDataStreamer>> {
        todo!()
    }

    async fn new_private_stream(
        &self,
        _ctx: PrivateBotInitContext,
    ) -> broker_core::error::Result<Box<BrokerageAccountDataStreamer>> {
        todo!()
    }
}

exchange!(Exchange::Coinbase, CoinbaseExchangeConnector);
