//! Use this module to interact with Bittrex exchange.
//! See examples for more informations.

#![feature(used_with_arg)]

#[macro_use]
extern crate broker_core;
#[macro_use]
extern crate tracing;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate anyhow;

use broker_core::fees::FeeProvider;
use serde_json::Value;
use std::sync::Arc;

use broker_core::prelude::*;

mod api;
mod generic_api;
mod models;
mod streaming_api;
mod utils;

pub use self::api::BittrexApi;
pub use self::streaming_api::BittrexStreamingApi;

#[async_trait(?Send)]
impl BrokerConnector for BittrexExchangeConnector {
    async fn new_api(&self, ctx: BrokerageInitContext) -> broker_core::error::Result<Arc<dyn Brokerage>> {
        Ok(Arc::new(BittrexApi::new(ctx.creds.as_ref())?))
    }

    async fn new_public_stream(
        &self,
        ctx: BrokerageBotInitContext,
    ) -> broker_core::error::Result<Box<MarketDataStreamer>> {
        Ok(Box::new(
            BittrexStreamingApi::new_bot(ctx.creds.as_ref(), ctx.channels.clone()).await?,
        ))
    }

    async fn new_private_stream(
        &self,
        _ctx: PrivateBotInitContext,
    ) -> broker_core::error::Result<Box<BrokerageAccountDataStreamer>> {
        todo!()
    }

    fn fees_provider(&self, _conf: Value) -> broker_core::error::Result<Arc<dyn FeeProvider>> { todo!() }
}

exchange!(Exchange::Bittrex, BittrexExchangeConnector);
