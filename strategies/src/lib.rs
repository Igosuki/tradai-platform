#![deny(unused_must_use, unused_mut, unused_imports, unused_import_braces)]
// TODO: See regression in nightly: https://github.com/rust-lang/rust/issues/70814
#![allow(unused_braces)]
#![allow(incomplete_features)]
#![feature(test)]
#![feature(async_closure)]
#![feature(impl_trait_in_bindings)]
#![feature(type_alias_impl_trait)]
#![feature(min_type_alias_impl_trait)]
#![feature(in_band_lifetimes)]
#![feature(inherent_associated_types)]
#![feature(fn_traits)]

extern crate log;
#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate derivative;
#[macro_use]
extern crate measure_time;
#[macro_use]
extern crate prometheus;

#[cfg(test)]
#[macro_use]
extern crate lazy_static;

use actix::{Actor, Addr, Context, Handler, ResponseActFuture, Running, WrapFuture};
use anyhow::Result;
use async_std::sync::Arc;
use async_std::sync::RwLock;
use coinnect_rt::exchange::Exchange;
use coinnect_rt::types::{LiveEventEnveloppe, Pair};
use derive_more::Display;
use serde::Deserialize;
use std::str::FromStr;
use strum_macros::EnumString;
use uuid::Uuid;

use crate::mean_reverting::options::Options as MeanRevertingStrategyOptions;
use crate::naive_pair_trading::options::Options as NaiveStrategyOptions;
use crate::order_manager::OrderManager;
use crate::query::{DataQuery, DataResult, FieldMutation};

pub mod error;
pub mod input;
pub mod mean_reverting;
mod models;
pub mod naive_pair_trading;
pub mod order_manager;
pub mod order_types;
pub mod query;
#[cfg(test)]
mod test_util;
mod types;
mod util;
mod wal;

#[derive(Clone)]
pub enum Channel {
    Orders { xch: Exchange, pair: Pair },
    Trades { xch: Exchange, pair: Pair },
    Orderbooks { xch: Exchange, pair: Pair },
}

#[derive(Eq, PartialEq, Hash, Clone, Debug, Deserialize, EnumString, Display)]
pub enum StrategyType {
    #[strum(serialize = "naive")]
    Naive,
    #[strum(serialize = "mean_reverting")]
    MeanReverting,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StrategyKey(pub StrategyType, pub String);

impl StrategyKey {
    pub fn from(t: &str, k: &str) -> Option<Self> {
        let st = StrategyType::from_str(t).ok()?;
        Some(Self(st, k.to_string()))
    }
}

#[derive(Clone)]
pub struct Strategy(pub StrategyKey, pub Addr<StrategyActor>, pub Vec<Channel>);

impl Strategy {
    pub fn new(db_path: Arc<String>, fees: f64, settings: StrategySettings, om: Option<Addr<OrderManager>>) -> Self {
        let strategy = from_settings(db_path.as_ref(), fees, &settings, om);
        let channels = strategy.channels();
        Self(
            settings.key(),
            { StrategyActor::start(StrategyActor::new(StrategyActorOptions { strategy })) },
            channels,
        )
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum StrategySettings {
    Naive(NaiveStrategyOptions),
    MeanReverting(MeanRevertingStrategyOptions),
}

impl StrategySettings {
    pub fn exchange(&self) -> Exchange {
        match self {
            Self::Naive(s) => s.exchange,
            Self::MeanReverting(s) => s.exchange,
        }
    }

    pub fn key(&self) -> StrategyKey {
        match &self {
            StrategySettings::Naive(n) => StrategyKey(StrategyType::Naive, format!("{}_{}", n.left, n.right)),
            StrategySettings::MeanReverting(n) => StrategyKey(StrategyType::MeanReverting, format!("{}", n.pair)),
        }
    }
}

pub struct StrategyActorOptions {
    pub strategy: Box<dyn StrategyInterface>,
}

pub struct StrategyActor {
    _session_uuid: Uuid,
    inner: Arc<RwLock<Box<dyn StrategyInterface>>>,
}

unsafe impl Send for StrategyActor {}

impl StrategyActor {
    pub fn new(options: StrategyActorOptions) -> Self {
        Self {
            _session_uuid: Uuid::new_v4(),
            inner: Arc::new(RwLock::new(options.strategy)),
        }
    }
}

impl Actor for StrategyActor {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        info!("Strategy actor starting");
    }
    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        info!("Strategy actor stopping");
        Running::Stop
    }
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("Strategy actor stopped, flushing strats...");
    }
}

type StratActorResponseFuture<T> = ResponseActFuture<StrategyActor, T>;
impl Handler<LiveEventEnveloppe> for StrategyActor {
    type Result = StratActorResponseFuture<anyhow::Result<()>>;

    #[cfg_attr(feature = "flame_it", flame)]
    fn handle(&mut self, msg: LiveEventEnveloppe, _ctx: &mut Self::Context) -> Self::Result {
        let lock = self.inner.clone();
        Box::pin(
            async move {
                let arc = lock.clone();
                let mut act = arc.write().await;
                act.add_event(msg).await
            }
            .into_actor(self),
        )
    }
}

impl Handler<DataQuery> for StrategyActor {
    type Result = StratActorResponseFuture<<DataQuery as actix::Message>::Result>;

    #[cfg_attr(feature = "flame_it", flame)]
    fn handle(&mut self, msg: DataQuery, _ctx: &mut Self::Context) -> Self::Result {
        let lock = self.inner.clone();
        Box::pin(
            async move {
                let mut act = lock.write().await;
                Ok(act.data(msg))
            }
            .into_actor(self),
        )
    }
}

impl Handler<FieldMutation> for StrategyActor {
    type Result = StratActorResponseFuture<<FieldMutation as actix::Message>::Result>;

    #[cfg_attr(feature = "flame_it", flame)]
    fn handle(&mut self, msg: FieldMutation, _ctx: &mut Self::Context) -> Self::Result {
        let lock = self.inner.clone();
        Box::pin(
            async move {
                let mut act = lock.write().await;
                act.mutate(msg)
            }
            .into_actor(self),
        )
    }
}

// TODO: strategies should define when to stop trading
#[async_trait]
pub trait StrategyInterface {
    async fn add_event(&mut self, le: LiveEventEnveloppe) -> anyhow::Result<()>;

    fn data(&mut self, q: DataQuery) -> Option<DataResult>;

    fn mutate(&mut self, m: FieldMutation) -> Result<()>;

    fn channels(&self) -> Vec<Channel>;
}

pub fn from_settings(
    db_path: &str,
    fees: f64,
    s: &StrategySettings,
    om: Option<Addr<OrderManager>>,
) -> Box<dyn StrategyInterface> {
    match s {
        StrategySettings::Naive(n) => {
            if let Some(o) = om {
                Box::new(crate::naive_pair_trading::NaiveTradingStrategy::new(
                    db_path, fees, n, o,
                ))
            } else {
                error!("Expected an order manager to be available for the targeted exchange of this NaiveStrategy");
                panic!();
            }
        }
        StrategySettings::MeanReverting(n) => {
            if let Some(o) = om {
                Box::new(crate::mean_reverting::MeanRevertingStrategy::new(db_path, fees, n, o))
            } else {
                error!(
                    "Expected an order manager to be available for the targeted exchange of this MeanRevertingStrategy"
                );
                panic!();
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::thread;

    use actix::System;

    use coinnect_rt::exchange::Exchange;
    use coinnect_rt::types::{LiveEvent, Orderbook};

    use super::*;
    use coinnect_rt::exchange::Exchange::Binance;

    fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

    fn actor(strategy: Box<dyn StrategyInterface>) -> StrategyActor {
        StrategyActor::new(StrategyActorOptions { strategy })
    }

    struct DummyStrat;

    #[async_trait]
    impl StrategyInterface for DummyStrat {
        async fn add_event(&mut self, _: LiveEventEnveloppe) -> anyhow::Result<()> { Ok(()) }

        fn data(&mut self, _q: DataQuery) -> Option<DataResult> { unimplemented!() }

        fn mutate(&mut self, _m: FieldMutation) -> anyhow::Result<()> { unimplemented!() }

        fn channels(&self) -> Vec<Channel> {
            vec![Channel::Orderbooks {
                xch: Binance,
                pair: "BTC_USDT".into(),
            }]
        }
    }

    /// TODO: this test does nothing
    #[test]
    fn test_workflow() {
        init();
        System::new().block_on(async move {
            let addr = StrategyActor::start(actor(Box::new(DummyStrat)));
            let order_book_event = LiveEventEnveloppe {
                xch: Exchange::Binance,
                e: LiveEvent::LiveOrderbook(Orderbook {
                    timestamp: chrono::Utc::now().timestamp(),
                    pair: "BTC_USDT".into(),
                    asks: vec![(0.1, 0.1), (0.2, 0.2)],
                    bids: vec![(0.1, 0.1), (0.2, 0.2)],
                    last_order_id: None,
                }),
            };
            println!("Sending...");
            for _ in 0..1000 {
                addr.do_send(order_book_event.clone());
            }
            thread::sleep(std::time::Duration::from_secs(1));
            System::current().stop();
        });
    }
}
