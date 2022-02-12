//! Use this module to interact with Bitstamp exchange.

#[macro_use]
extern crate broker_core;
#[macro_use]
extern crate tracing;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate actix;

use broker_core::bot::ExchangeBot;
use broker_core::broker::MarketEventEnvelopeMsg;
use broker_core::prelude::*;
use std::sync::Arc;

mod account_api;
#[cfg(feature = "test_util")]
pub mod adapters;
#[cfg(not(feature = "test_util"))]
mod adapters;
mod api;
mod generic_api;
mod streaming_api;

pub use self::account_api::BinanceStreamingAccountApi;
pub use self::api::BinanceApi;
pub use self::streaming_api::BinanceStreamingApi;

#[async_trait(?Send)]
impl ExchangeConnector for BinanceExchangeConnector {
    async fn new_api(&self, ctx: ExchangeApiInitContext) -> broker_core::error::Result<Arc<dyn ExchangeApi>> {
        let api: Arc<dyn ExchangeApi> = Arc::new(if ctx.use_test_servers {
            BinanceApi::new_test(ctx.creds.as_ref()).await?
        } else {
            BinanceApi::new(ctx.creds.as_ref()).await?
        });
        Ok(api)
    }

    async fn new_public_stream(
        &self,
        ctx: ExchangeBotInitContext,
    ) -> broker_core::error::Result<Box<MarketExchangeBot>> {
        let b: Box<dyn ExchangeBot<MarketEventEnvelopeMsg>> = Box::new(
            BinanceStreamingApi::try_new(
                ctx.creds.as_ref(),
                ctx.channels,
                ctx.settings.use_test,
                ctx.settings.orderbook_depth,
            )
            .await?,
        );
        Ok(b)
    }

    async fn new_private_stream(
        &self,
        _ctx: PrivateBotInitContext,
    ) -> broker_core::error::Result<Box<AccountExchangeBot>> {
        todo!()
    }
}

exchange!(Exchange::Binance, BinanceExchangeConnector);
