//! Use this module to interact with Bitstamp exchange.

#![feature(used_with_arg)]

#[macro_use]
extern crate broker_core;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate actix;
#[macro_use]
extern crate anyhow;

use broker_core::fees::FeeProvider;
use broker_core::prelude::*;
use serde_json::Value;
use std::sync::Arc;

mod api;
mod generic_api;
mod models;
mod streaming_api;
mod utils;

use self::api::BitstampApi;
use self::streaming_api::BitstampStreamingApi;

#[async_trait(?Send)]
impl BrokerConnector for BitstampExchangeConnector {
    async fn new_api(&self, ctx: BrokerageInitContext) -> broker_core::error::Result<Arc<dyn Brokerage>> {
        Ok(Arc::new(BitstampApi::new(ctx.creds.as_ref())?))
    }

    async fn new_public_stream(
        &self,
        ctx: BrokerageBotInitContext,
    ) -> broker_core::error::Result<Box<MarketDataStreamer>> {
        Ok(Box::new(
            BitstampStreamingApi::new_bot(ctx.creds.as_ref(), ctx.channels).await?,
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

exchange!(Exchange::Bitstamp, BitstampExchangeConnector);
