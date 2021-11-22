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
#[cfg(feature = "python")]
#[macro_use]
extern crate pyo3;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate tracing;

use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use actix::{Addr, Message, Recipient};
use derive_more::Display;
use serde::Deserialize;
use strum_macros::AsRefStr;
use strum_macros::EnumString;
use uuid::Uuid;

use actor::StrategyActor;
pub use coinnect_rt::margin_interest_rates;
use coinnect_rt::margin_interest_rates::MarginInterestRateProvider;
pub use coinnect_rt::prelude::*;
pub use coinnect_rt::types as coinnect_types;
pub use db::DbOptions;
use error::*;
use ext::ResultExt;
pub use models::Model;
#[cfg(feature = "python")]
pub use python_wrapper::python_strat;
pub use settings::{StrategyCopySettings, StrategySettings};

use crate::actor::StrategyActorOptions;
use crate::query::{DataQuery, DataResult};
use crate::types::StratEvent;
use trading::order_manager::OrderManager;

pub mod actor;
pub mod driver;
pub mod error;
mod generic;
pub mod mean_reverting;
mod models;
pub mod naive_pair_trading;
mod python_wrapper;
pub mod query;
mod repos;
pub mod settings;
#[cfg(test)]
mod test_util;
pub mod types;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Channel {
    Orders { xch: Exchange, pair: Pair },
    Trades { xch: Exchange, pair: Pair },
    Orderbooks { xch: Exchange, pair: Pair },
}

impl Channel {
    pub fn exchange(&self) -> Exchange {
        match self {
            Channel::Orders { xch, .. } => *xch,
            Channel::Trades { xch, .. } => *xch,
            Channel::Orderbooks { xch, .. } => *xch,
        }
    }

    pub fn pair(&self) -> Pair {
        match self {
            Channel::Orders { pair, .. } => pair.clone(),
            Channel::Trades { pair, .. } => pair.clone(),
            Channel::Orderbooks { pair, .. } => pair.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, AsRefStr, juniper::GraphQLEnum)]
#[serde(rename_all = "snake_case")]
pub enum StrategyStatus {
    #[strum(serialize = "stopped")]
    Stopped,
    #[strum(serialize = "running")]
    Running,
    #[strum(serialize = "not_trading")]
    NotTrading,
}

#[derive(actix::Message, juniper::GraphQLEnum)]
#[rtype(result = "Result<StrategyStatus>")]
pub enum StrategyLifecycleCmd {
    Restart,
    StopTrading,
    ResumeTrading,
}

#[derive(Eq, PartialEq, Hash, Clone, Debug, Deserialize, EnumString, Display, AsRefStr)]
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

impl ToString for StrategyKey {
    fn to_string(&self) -> String { format!("{}_{}", self.0, self.1) }
}

/// A trader owns the context of running a single strategy and is responsible for managing
/// its data interface and lifecycle
#[derive(Clone)]
pub struct Trader {
    pub key: StrategyKey,
    actor: Addr<StrategyActor>,
    pub channels: Vec<Channel>,
}

impl Trader {
    // TODO: om, mirp and fees could be in a single trading engine struct
    pub fn new(
        db: &DbOptions<String>,
        exchange_conf: &ExchangeSettings,
        actor_settings: &StrategyActorOptions,
        settings: &StrategySettings,
        om: Option<Addr<OrderManager>>,
        mirp: Addr<MarginInterestRateProvider>,
        logger: Option<Arc<dyn StratEventLogger>>,
    ) -> Self {
        let uuid = Uuid::new_v4();
        let key = settings.key();
        let db = db.clone();
        let settings = settings.clone();
        let exchange_conf = exchange_conf.clone();
        let actor = StrategyActor::new_with_uuid(
            Box::new(move || {
                settings::from_settings(&db, &exchange_conf, &settings, om.clone(), mirp.clone(), logger.clone())
            }),
            actor_settings,
            uuid,
        );
        let channels = actor.channels();
        info!(uuid = %uuid, channels = ?channels, "starting strategy");
        Self {
            key,
            actor: actix::Supervisor::start(|_| actor),
            channels,
        }
    }

    pub fn live_event_recipient(&self) -> Recipient<Arc<LiveEventEnvelope>> { self.actor.clone().recipient() }

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
pub trait StratEventLogger: Sync + Send {
    async fn maybe_log(&self, event: Option<StratEvent>);
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    use actix::System;
    use futures::StreamExt;

    use coinnect_rt::prelude::*;

    use crate::driver::StrategyDriver;
    use crate::query::{ModelReset, Mutation};

    use super::*;

    fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

    const TEST_PAIR: &str = "BTC_USDT";

    struct LoggingStrat {
        log: Arc<Mutex<Vec<LiveEventEnvelope>>>,
    }

    impl LoggingStrat {
        fn new(log: Arc<Mutex<Vec<LiveEventEnvelope>>>) -> Self { Self { log } }
    }

    #[async_trait]
    impl StrategyDriver for LoggingStrat {
        async fn key(&self) -> String { "logging".to_string() }

        async fn add_event(&mut self, e: &LiveEventEnvelope) -> Result<()> {
            let mut g = self.log.lock().unwrap();
            g.push(e.clone());
            Ok(())
        }

        fn data(&mut self, _: DataQuery) -> Result<DataResult> { Ok(DataResult::Success(true)) }

        fn mutate(&mut self, _: Mutation) -> Result<()> { Ok(()) }

        fn channels(&self) -> Vec<Channel> {
            vec![Channel::Orderbooks {
                xch: Exchange::Binance,
                pair: TEST_PAIR.into(),
            }]
        }

        fn stop_trading(&mut self) {}

        fn resume_trading(&mut self) {}

        async fn resolve_orders(&mut self) { todo!() }
    }

    #[test]
    fn test_workflow() {
        init();
        System::new().block_on(async move {
            let order_book_event = LiveEventEnvelope {
                xch: Exchange::Binance,
                e: LiveEvent::LiveOrderbook(Orderbook {
                    timestamp: chrono::Utc::now().timestamp(),
                    pair: TEST_PAIR.into(),
                    asks: vec![(0.1, 0.1), (0.2, 0.2)],
                    bids: vec![(0.1, 0.1), (0.2, 0.2)],
                    last_order_id: None,
                }),
            };
            let log = Arc::new(Mutex::new(vec![]));
            let events: Vec<LiveEventEnvelope> = std::iter::repeat(order_book_event).take(10).collect();
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
