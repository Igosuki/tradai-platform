/*!
Contains the Strategy API, which defines plumbing code to manage drivers, state, queries, and models

# Overview

The `Strategy` interface in this crate can be implemented to add a new runnable trading strategy.
Typically, a strategy is wrapped in a driver, which is handled by an actor.
All of this behavior can be overridden by implementing the common traits for the `StrategyDriver` and `StrategyActor`.

# Components

## Driver

The driver is responsible for invoking and managing the lifecycle of the strategy, as well as any outside interaction.
It also passes orders, manages the portfolio, and handles strategy events.

## Strategy

A strategy defines its model, which may or may not be indicator based, and emits trading signals. While the strategy
can define any custom behavior, it is best to use the driver and already defined to override predefined behavior.

## Queries

Through a single method, drivers can receive queries and mutations to get the specific state or trade history of a single strategy.

## Models

Specific APIs exist to facilitate persisting time based statistical models.

 */

#![deny(unused_must_use, unused_mut, unused_imports, unused_import_braces)]
// TODO: See regression in nightly: https://github.com/rust-lang/rust/issues/70814
#![allow(unused_braces)]
#![allow(incomplete_features)]
#![allow(
    clippy::wildcard_imports,
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::missing_errors_doc
)]
#![feature(test)]
#![feature(async_closure)]
#![feature(type_alias_impl_trait)]
#![feature(in_band_lifetimes)]
#![feature(inherent_associated_types)]
#![feature(fn_traits)]
#![feature(generic_associated_types)]
#![feature(const_fn_fn_ptr_basics)]
#![feature(const_fn_trait_bound)]

#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate derivative;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate tracing;

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use actix::{Addr, Message, Recipient};
use chrono::Duration;
use serde::Deserialize;
use strum_macros::AsRefStr;
use uuid::Uuid;

use actor::StrategyActor;
use brokers::broker::{MarketEventEnvelopeMsg, Subject};
use brokers::prelude::*;
use brokers::types::{SecurityType, Symbol};
use db::DbOptions;
use error::*;
use ext::ResultExt;
use stats::kline::Resolution;
use trading::engine::TradingEngine;
use util::time::TimedData;

use crate::actor::StrategyActorOptions;
use crate::plugin::{StrategyPlugin, StrategyPluginRegistry};
use crate::prelude::StrategyDriverSettings;
use crate::types::StratEvent;

pub mod prelude {
    pub use super::generic::{GenericDriver, GenericDriverOptions, PortfolioOptions};
    pub use super::models::Model;
    pub use super::settings::{StrategyCopySettings, StrategyDriverSettings, StrategySettings};
    pub use super::types::StratEvent;
    pub use super::EventLogger;
}

pub mod actor;
pub mod driver;
pub mod error;
pub mod event;
mod generic;
pub mod models;
pub mod plugin;
pub mod query;
pub mod settings;
#[cfg(test)]
mod test_util;
pub mod types;

/// A market channel represents a unique stream of data that will be required to run a strategy
/// Historical and Real-Time data will be provided from this on a best effort basis.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MarketChannel {
    /// A unique identifier for the security requested by this market data channel
    symbol: Symbol,
    /// The type of the ticker
    r#type: MarketChannelType,
    /// The minimal tick rate for the data, in reality max(tick_rate, exchange_tick_rate) will be used
    tick_rate: Option<Duration>,
    /// If set, the data will be aggregated in OHLCV candles
    resolution: Option<Resolution>,
    /// Only send final candles
    only_final: Option<bool>,
}

impl MarketChannel {
    pub fn exchange(&self) -> Exchange { self.symbol.id.xch }

    pub fn pair(&self) -> Pair { self.symbol.id.symbol }

    pub fn name(&self) -> &'static str {
        match self.r#type {
            MarketChannelType::Trades => "trades",
            MarketChannelType::Orderbooks => "order_books",
            MarketChannelType::OpenInterest => "interests",
            MarketChannelType::Candles => "candles",
            MarketChannelType::Quotes => "quotes",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MarketChannelType {
    /// Raw Trades see [MarketEvent::Trade]
    Trades,
    /// Order book changes see [MarketEvent::Orderbook]
    Orderbooks,
    /// Kline events see [MarketEvent::CandleTick]
    Candles,
    /// Open interest for futures see [MarketEvent::OpenInterest]
    OpenInterest,
    /// Order book quotes see [MarketEvent::Quote]
    Quotes,
}

impl From<&MarketEventEnvelope> for MarketChannel {
    fn from(msg: &MarketEventEnvelope) -> Self {
        Self {
            xch: msg.xch,
            pair: msg.pair.clone(),
            r#type: match msg.e {
                MarketEvent::Trade(_) => MarketChannelType::Trades,
                MarketEvent::Orderbook(_) => MarketChannelType::Orderbooks,
                MarketEvent::CandleTick(_) => MarketChannelType::Candles,
            },
            sec_type: msg.sec_type,
            tick_rate: None,
            resolution: None,
            only_final: None,
        }
    }
}

impl From<MarketEventEnvelopeMsg> for MarketChannel {
    fn from(msg: MarketEventEnvelopeMsg) -> Self { Self::from(msg.as_ref()) }
}

impl From<MarketEventEnvelope> for MarketChannel {
    fn from(msg: MarketEventEnvelope) -> Self { Self::from(&msg) }
}

impl Subject<MarketEventEnvelopeMsg> for MarketChannel {}

impl Subject<MarketEventEnvelope> for MarketChannel {}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, AsRefStr, juniper::GraphQLEnum)]
#[serde(rename_all = "snake_case")]
pub enum StrategyStatus {
    /// Stopped, no instances are running
    #[strum(serialize = "stopped")]
    Stopped,
    /// Is running without issues
    #[strum(serialize = "running")]
    Running,
    /// Stopped trading for custom reasons
    #[strum(serialize = "not_trading")]
    NotTrading,
    /// Did not initialize properly
    #[strum(serialize = "deploy_error")]
    DeployError,
    /// Positions got liquidated
    #[strum(serialize = "liquidated")]
    Liquidated,
    /// Ran to completion without errors
    #[strum(serialize = "completed")]
    Completed,
}

impl Default for StrategyStatus {
    fn default() -> Self { Self::Running }
}

#[derive(actix::Message, juniper::GraphQLEnum)]
#[rtype(result = "Result<StrategyStatus>")]
pub enum StrategyLifecycleCmd {
    Restart,
    StopTrading,
    ResumeTrading,
}

/// Strategy type, followed by a unique key
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StrategyKey(pub String, pub String);

impl StrategyKey {
    pub fn from(t: &str, k: &str) -> Option<Self> {
        // TODO: check type exists in plugin registry
        Some(Self(t.to_string(), k.to_string()))
    }
}

impl ToString for StrategyKey {
    fn to_string(&self) -> String { format!("{}_{}", self.0, self.1) }
}

pub type StratEventLoggerRef = Arc<dyn EventLogger<TimedData<StratEvent>>>;

/// A trader owns the context of running a single strategy and is responsible for managing
/// its data interface and lifecycle
#[derive(Clone)]
pub struct Trader {
    pub key: StrategyKey,
    actor: Addr<StrategyActor>,
    pub channels: HashSet<MarketChannel>,
}

impl Trader {
    /// # Panics
    ///
    /// if creating the strategy fails
    pub fn try_new(
        plugins: &StrategyPluginRegistry<'static>,
        db_opts: &DbOptions<String>,
        actor_settings: &StrategyActorOptions,
        settings: &StrategyDriverSettings,
        engine: Arc<TradingEngine>,
        logger: Option<StratEventLoggerRef>,
    ) -> Result<Self> {
        let strat_type = settings.strat.strat_type.clone();
        let plugin: &'static StrategyPlugin = plugins.get(strat_type.as_str()).ok_or(Error::StrategyPluginNotFound)?;
        let uuid = Uuid::new_v4();
        let key = plugin.options(settings.strat.options.clone())?.key();
        let settings = settings.clone();
        let db_opts = db_opts.clone();
        let actor = StrategyActor::new_with_uuid(
            Box::new(move || {
                settings::from_driver_settings(plugin, &db_opts, &settings, engine.clone(), logger.clone()).unwrap()
            }),
            actor_settings,
            uuid,
        );
        let channels = actor.channels();
        info!(uuid = %uuid, channels = ?channels, "starting strategy");
        Ok(Self {
            key,
            actor: actix::Supervisor::start(|_| actor),
            channels,
        })
    }

    pub fn market_event_recipient(&self) -> Recipient<MarketEventEnvelopeMsg> { self.actor.clone().recipient() }

    pub async fn send<M: 'static>(&self, m: M) -> Result<<M as Message>::Result>
    where
        M: Message + Send,
        M::Result: Send + Debug,
        StrategyActor: actix::Handler<M>,
    {
        self.actor.send(m).await.err_into()
    }
}

#[async_trait]
pub trait EventLogger<T>: Sync + Send + Debug {
    async fn log(&self, event: T);
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    use actix::System;
    use futures::StreamExt;

    use brokers::prelude::*;

    use crate::driver::StrategyDriver;
    use crate::query::{DataQuery, DataResult, ModelReset, Mutation};

    use super::*;

    fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

    const TEST_PAIR: &str = "BTC_USDT";

    struct LoggingStrat {
        log: Arc<Mutex<Vec<MarketEventEnvelope>>>,
    }

    impl LoggingStrat {
        fn new(log: Arc<Mutex<Vec<MarketEventEnvelope>>>) -> Self { Self { log } }
    }

    #[async_trait]
    impl StrategyDriver for LoggingStrat {
        async fn init(&mut self) -> Result<()> { todo!() }

        async fn key(&self) -> String { "logging".to_string() }

        async fn add_event(&mut self, e: &MarketEventEnvelope) -> Result<()> {
            let mut g = self.log.lock().unwrap();
            g.push(e.clone());
            Ok(())
        }

        async fn data(&mut self, _: DataQuery) -> Result<DataResult> { Ok(DataResult::Success(true)) }

        fn mutate(&mut self, _: Mutation) -> Result<()> { Ok(()) }

        fn channels(&self) -> HashSet<MarketChannel> {
            vec![MarketChannel::Orderbooks {
                xch: Exchange::Binance,
                pair: TEST_PAIR.into(),
            }]
            .into_iter()
            .collect()
        }

        fn stop_trading(&mut self) -> Result<()> { todo!() }

        fn resume_trading(&mut self) -> Result<()> { todo!() }

        async fn resolve_orders(&mut self) { todo!() }

        async fn is_locked(&self) -> bool { false }
    }

    #[test]
    fn test_workflow() {
        init();
        System::new().block_on(async move {
            let order_book_event = MarketEventEnvelope::order_book_event(
                Exchange::Binance,
                TEST_PAIR.into(),
                chrono::Utc::now().timestamp(),
                vec![(0.1, 0.1), (0.2, 0.2)],
                vec![(0.1, 0.1), (0.2, 0.2)],
            );
            let log = Arc::new(Mutex::new(vec![]));
            let events: Vec<MarketEventEnvelope> = std::iter::repeat(order_book_event).take(10).collect();
            let log_a = log.clone();
            let options = StrategyActorOptions::default();
            let addr = actix::Supervisor::start(move |_| {
                StrategyActor::new(Box::new(move || Box::new(LoggingStrat::new(log_a.clone()))), &options)
            });
            for event in events.clone() {
                addr.send(Arc::new(event)).await.unwrap().unwrap();
            }
            let log = log.lock().unwrap().clone();
            assert_eq!(log, events);
            //let r = addr.send(StrategyLifecycleCmd::Restart).await.unwrap();
            //assert_eq!(r.ok(), Some(StrategyStatus::Running));
            assert!(addr.connected());
            let r = addr.send(DataQuery::Status).await.unwrap().unwrap();
            assert_eq!(r, Some(DataResult::Success(true)));
            let r = addr.send(ModelReset::default()).await.unwrap();
            assert!(r.is_ok());
            tokio_stream::iter(0..10)
                .for_each(|_| tokio::time::sleep(Duration::from_millis(100)))
                .await;
            System::current().stop();
            thread::sleep(std::time::Duration::from_secs(1));
        });
    }
}
