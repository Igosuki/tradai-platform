//! Use this module to interact with Poloniex exchange.

#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate broker_core;
#[macro_use]
extern crate anyhow;

use std::sync::Arc;

use broker_core::prelude::*;

mod api;
mod generic_api;
mod utils;

pub use self::api::PoloniexApi;
pub use self::api::{MoveOrderOption, PlaceOrderOption};

#[async_trait(?Send)]
impl BrokerConnector for PoloniexExchangeConnector {
    async fn new_api(&self, ctx: BrokerageInitContext) -> broker_core::error::Result<Arc<dyn Brokerage>> {
        Ok(Arc::new(PoloniexApi::new(ctx.creds.as_ref())?))
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

exchange!(Exchange::Poloniex, PoloniexExchangeConnector);
