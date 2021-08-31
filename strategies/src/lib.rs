#![deny(unused_must_use, unused_mut, unused_imports, unused_import_braces)]
// TODO: See regression in nightly: https://github.com/rust-lang/rust/issues/70814
#![allow(unused_braces)]
#![allow(incomplete_features)]
#![feature(test)]
#![feature(async_closure)]
#![feature(type_alias_impl_trait)]
#![feature(in_band_lifetimes)]
#![feature(inherent_associated_types)]
#![feature(fn_traits)]
#![feature(result_cloned)]
#![feature(macro_attributes_in_derive_output)]

#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate derivative;
#[cfg(test)]
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate tracing;
#[macro_use]
extern crate pyo3;

use std::str::FromStr;

use actix::{Actor, ActorContext, ActorFutureExt, Addr, Context, Handler, ResponseActFuture, WrapFuture};
use async_std::sync::Arc;
use async_std::sync::RwLock;
use derive_more::Display;
use serde::Deserialize;
use strum_macros::EnumString;
use uuid::Uuid;

use coinnect_rt::exchange::Exchange;
pub use coinnect_rt::types as coinnect_types;
use coinnect_rt::types::{LiveEventEnvelope, Pair};
pub use db::DbOptions;
use error::*;
pub use models::Model;
pub use settings::{StrategyCopySettings, StrategySettings};

use crate::order_manager::OrderManager;
use crate::query::{DataQuery, DataResult, ModelReset, Mutation, StateFieldMutation};

pub mod error;
mod generic;
pub mod input;
pub mod mean_reverting;
mod models;
pub mod naive_pair_trading;
pub mod order_manager;
pub mod order_types;
pub mod query;
pub mod settings;
#[cfg(test)]
mod test_util;
pub mod types;
mod util;
mod wal;

use backoff::ExponentialBackoff;
pub use generic::python_strat;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Channel {
    Orders { xch: Exchange, pair: Pair },
    Trades { xch: Exchange, pair: Pair },
    Orderbooks { xch: Exchange, pair: Pair },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, juniper::GraphQLEnum)]
#[serde(rename_all = "snake_case")]
pub enum StrategyStatus {
    Stopped,
    Running,
    NotTrading,
}

#[derive(actix::Message)]
#[rtype(result = "Result<StrategyStatus>")]
pub enum StrategyLifecycleCmd {
    Stop,
    Restart,
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
    pub fn new(db: &DbOptions<String>, fees: f64, settings: &StrategySettings, om: Option<Addr<OrderManager>>) -> Self {
        let uuid = Uuid::new_v4();
        let key = settings.key();
        let db = db.clone();
        let settings = settings.clone();
        let actor = StrategyActor::new_with_uuid(
            Box::new(move || settings::from_settings(&db, fees, &settings, om.clone())),
            uuid,
        );
        let channels = actor.channels();
        info!(uuid = %uuid, channels = ?channels, "starting strategy");
        Self(key, actix::Supervisor::start(|_| actor), channels)
    }
}

pub type StrategySpawner = dyn Fn() -> Box<dyn StrategyDriver>;

pub struct StrategyActor {
    session_uuid: Uuid,
    spawner: Box<StrategySpawner>,
    inner: Arc<RwLock<Box<dyn StrategyDriver>>>,
    #[allow(dead_code)]
    conn_backoff: ExponentialBackoff,
    channels: Vec<Channel>,
}

unsafe impl Send for StrategyActor {}

impl StrategyActor {
    pub fn new(spawner: Box<StrategySpawner>) -> Self { Self::new_with_uuid(spawner, Uuid::new_v4()) }

    pub fn new_with_uuid(spawner: Box<StrategySpawner>, session_uuid: Uuid) -> Self {
        let inner = spawner();
        let channels = inner.channels();
        let inner = Arc::new(RwLock::new(inner));
        Self {
            session_uuid,
            spawner,
            inner,
            channels,
            conn_backoff: ExponentialBackoff {
                max_elapsed_time: Some(Duration::from_secs(5)),
                ..ExponentialBackoff::default()
            },
        }
    }

    fn channels(&self) -> Vec<Channel> { self.channels.clone() }
}

impl Actor for StrategyActor {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        info!(uuid = %self.session_uuid, "strategy started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(uuid = %self.session_uuid, "strategy stopped, flushing...");
    }
}

impl actix::Supervised for StrategyActor {
    fn restarting(&mut self, _ctx: &mut <Self as Actor>::Context) {
        info!(session_uuid = %self.session_uuid, "strategy restarting");
        self.inner = Arc::new(RwLock::new((self.spawner)()));
    }
}

type StratActorResponseFuture<T> = ResponseActFuture<StrategyActor, T>;

impl Handler<Arc<LiveEventEnvelope>> for StrategyActor {
    type Result = StratActorResponseFuture<anyhow::Result<()>>;

    #[cfg_attr(feature = "flame_it", flame)]
    fn handle(&mut self, msg: Arc<LiveEventEnvelope>, _ctx: &mut Self::Context) -> Self::Result {
        let lock = self.inner.clone();
        Box::pin(
            async move {
                let mut inner = lock.write().await;
                inner.add_event(msg.as_ref()).await.map_err(|e| anyhow!(e))
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
                let mut inner = lock.write().await;
                Ok(inner.data(msg))
            }
            .into_actor(self),
        )
    }
}

impl Handler<StateFieldMutation> for StrategyActor {
    type Result = StratActorResponseFuture<<StateFieldMutation as actix::Message>::Result>;

    #[cfg_attr(feature = "flame_it", flame)]
    fn handle(&mut self, msg: StateFieldMutation, _ctx: &mut Self::Context) -> Self::Result {
        let lock = self.inner.clone();
        Box::pin(
            async move {
                let mut inner = lock.write().await;
                inner.mutate(Mutation::State(msg))
            }
            .into_actor(self),
        )
    }
}

impl Handler<ModelReset> for StrategyActor {
    type Result = StratActorResponseFuture<<ModelReset as actix::Message>::Result>;

    #[cfg_attr(feature = "flame_it", flame)]
    fn handle(&mut self, msg: ModelReset, _ctx: &mut Self::Context) -> Self::Result {
        let restart_after = msg.restart_after;
        let lock = self.inner.clone();
        Box::pin(
            async move {
                let mut inner = lock.write().await;
                if msg.stop_trading {
                    inner.toggle_trading();
                }
                inner.mutate(Mutation::Model(msg))
            }
            .into_actor(self)
            .map(move |_, _act, ctx| {
                if restart_after {
                    ctx.stop();
                    Ok(StrategyStatus::Stopped)
                } else {
                    Ok(StrategyStatus::Running)
                }
            }),
        )
    }
}

impl Handler<StrategyLifecycleCmd> for StrategyActor {
    type Result = Result<StrategyStatus>;

    fn handle(&mut self, msg: StrategyLifecycleCmd, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            StrategyLifecycleCmd::Stop => {
                ctx.stop();
                Ok(StrategyStatus::Stopped)
            }
            StrategyLifecycleCmd::Restart => {
                ctx.stop();
                Ok(StrategyStatus::Running)
            }
        }
    }
}

#[async_trait]
pub trait StrategyDriver {
    async fn add_event(&mut self, le: &LiveEventEnvelope) -> Result<()>;

    fn data(&mut self, q: DataQuery) -> Option<DataResult>;

    fn mutate(&mut self, m: Mutation) -> Result<()>;

    fn channels(&self) -> Vec<Channel>;

    fn toggle_trading(&mut self) -> bool;
}

#[cfg(test)]
mod test {
    use std::thread;

    use actix::System;

    use coinnect_rt::exchange::Exchange;
    use coinnect_rt::exchange::Exchange::Binance;
    use coinnect_rt::types::{LiveEvent, Orderbook};

    use super::*;

    fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

    fn actor(spawner: Box<StrategySpawner>) -> StrategyActor { StrategyActor::new(spawner) }

    #[derive(Clone)]
    struct DummyStrat;

    #[async_trait]
    impl StrategyDriver for DummyStrat {
        async fn add_event(&mut self, _: &LiveEventEnvelope) -> Result<()> { Ok(()) }

        fn data(&mut self, _q: DataQuery) -> Option<DataResult> { unimplemented!() }

        fn mutate(&mut self, _m: Mutation) -> Result<()> { unimplemented!() }

        fn channels(&self) -> Vec<Channel> {
            vec![Channel::Orderbooks {
                xch: Binance,
                pair: "BTC_USDT".into(),
            }]
        }

        fn toggle_trading(&mut self) -> bool { false }
    }

    #[test]
    fn test_workflow() {
        init();
        System::new().block_on(async move {
            let addr = actix::Supervisor::start(|_| actor(Box::new(move || Box::new(DummyStrat))));
            let order_book_event = Arc::new(LiveEventEnvelope {
                xch: Exchange::Binance,
                e: LiveEvent::LiveOrderbook(Orderbook {
                    timestamp: chrono::Utc::now().timestamp(),
                    pair: "BTC_USDT".into(),
                    asks: vec![(0.1, 0.1), (0.2, 0.2)],
                    bids: vec![(0.1, 0.1), (0.2, 0.2)],
                    last_order_id: None,
                }),
            });
            for _ in 0..10 {
                addr.do_send(order_book_event.clone());
            }
            let r = addr.send(StrategyLifecycleCmd::Restart).await.unwrap();
            assert_eq!(r.ok(), Some(StrategyStatus::Running));
            let r = addr.send(StrategyLifecycleCmd::Stop).await.unwrap();
            assert_eq!(r.ok(), Some(StrategyStatus::Stopped));
            thread::sleep(std::time::Duration::from_secs(1));
            System::current().stop();
        });
    }
}
