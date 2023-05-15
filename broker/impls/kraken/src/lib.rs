//! Use this module to interact with Kraken exchange.
//! See examples for more informations.

#![feature(used_with_arg)]

#[macro_use]
extern crate broker_core;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate serde;

use broker_core::fees::FeeProvider;
use serde_json::Value;
use std::sync::Arc;

use broker_core::prelude::*;

mod api;
mod generic_api;
mod model;
mod utils;

pub use self::api::KrakenApi;
pub use utils::{get_currency_enum, get_currency_string};

#[async_trait(?Send)]
impl BrokerConnector for KrakenExchangeConnector {
    async fn new_api(&self, ctx: BrokerageInitContext) -> broker_core::error::Result<Arc<dyn Brokerage>> {
        Ok(Arc::new(KrakenApi::new(ctx.creds.as_ref())?))
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

    fn fees_provider(&self, _conf: Value) -> broker_core::error::Result<Arc<dyn FeeProvider>> { todo!() }
}

exchange!(Exchange::Kraken, KrakenExchangeConnector);
