//! Use this module to interact with Bitstamp exchange.

#[macro_use]
extern crate broker_core;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate actix;
#[macro_use]
extern crate anyhow;

use broker_core::prelude::*;
use std::sync::Arc;

mod api;
mod generic_api;
mod models;
mod streaming_api;
mod utils;

use self::api::BitstampApi;
use self::streaming_api::BitstampStreamingApi;

#[async_trait(?Send)]
impl ExchangeConnector for BitstampExchangeConnector {
    async fn new_api(&self, ctx: ExchangeApiInitContext) -> broker_core::error::Result<Arc<dyn ExchangeApi>> {
        Ok(Arc::new(BitstampApi::new(ctx.creds.as_ref())?))
    }

    async fn new_public_stream(
        &self,
        ctx: ExchangeBotInitContext,
    ) -> broker_core::error::Result<Box<MarketExchangeBot>> {
        Ok(Box::new(
            BitstampStreamingApi::new_bot(ctx.creds.as_ref(), ctx.channels).await?,
        ))
    }

    async fn new_private_stream(
        &self,
        _ctx: PrivateBotInitContext,
    ) -> broker_core::error::Result<Box<AccountExchangeBot>> {
        todo!()
    }
}

exchange!(Exchange::Bitstamp, BitstampExchangeConnector);
