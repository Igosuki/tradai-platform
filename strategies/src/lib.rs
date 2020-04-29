#![feature(test)]

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate strum_macros;

use actix::{Actor, Handler, Running, SyncContext};
use chrono::Duration;
use coinnect_rt::exchange::Exchange;
use coinnect_rt::types::{LiveEvent, LiveEventEnveloppe, Pair};
use derive_more::Display;
use parse_duration::parse;
use serde::Deserialize;
use uuid::Uuid;
#[cfg(test)]
#[macro_use]
extern crate quickcheck;

pub mod naive_pair_trading;

#[derive(Clone, Debug, Deserialize)]
pub struct NaiveStrategy {
    pub left: Pair,
    pub right: Pair,
    pub exchange: Exchange,
    pub beta_eval_freq: i32,
    pub beta_sample_freq: String,
    pub window_size: i32,
    pub threshold_long: f64,
    pub threshold_short: f64,
    pub stop_loss: f64,
    pub stop_gain: f64,
}

impl NaiveStrategy {
    fn beta_sample_freq(&self) -> Duration {
        Duration::from_std(parse(&self.beta_sample_freq).unwrap()).unwrap()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
pub enum Strategy {
    Naive(NaiveStrategy),
}

impl Strategy {
    pub fn exchange(&self) -> Exchange {
        match self {
            Self::Naive(s) => s.exchange,
        }
    }
}

pub struct StrategyActorOptions {
    pub strategy: Box<dyn StrategySink>,
}

#[derive(Debug, Display)]
pub enum Error {
    IOError(std::io::Error),
}

impl std::error::Error for Error {}

pub struct StrategyActor {
    _session_uuid: Uuid,
    inner: Box<dyn StrategySink>,
}

impl StrategyActor {
    pub fn new(options: StrategyActorOptions) -> Self {
        Self {
            _session_uuid: Uuid::new_v4(),
            inner: options.strategy,
        }
    }
}

impl Actor for StrategyActor {
    type Context = SyncContext<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        info!("starting");
    }
    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        info!("stopping");
        Running::Stop
    }
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("Strategy actor stopped, flushing strats...");
    }
}

impl Handler<LiveEventEnveloppe> for StrategyActor {
    type Result = ();

    #[cfg_attr(feature = "flame_it", flame)]
    fn handle(&mut self, msg: LiveEventEnveloppe, _ctx: &mut Self::Context) -> Self::Result {
        self.inner.add_event(msg.1).unwrap();
    }
}

pub trait StrategySink {
    fn add_event(&mut self, le: LiveEvent) -> std::io::Result<()>;
}

pub fn from_settings(db_path: &str, fees: f64, s: &Strategy) -> Box<dyn StrategySink> {
    let s = match s {
        Strategy::Naive(n) => crate::naive_pair_trading::Strategy::new(db_path, fees, n),
    };
    Box::new(s)
}

#[cfg(test)]
mod test {
    use std::thread;

    use actix::SyncArbiter;
    use actix_rt::System;

    use coinnect_rt::exchange::Exchange;
    use coinnect_rt::types::Orderbook;
    use coinnect_rt::types::Pair;

    use super::*;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    fn actor(strategy: Box<dyn StrategySink>) -> StrategyActor {
        StrategyActor::new(StrategyActorOptions { strategy })
    }

    struct DummyStrat;
    impl StrategySink for DummyStrat {
        fn add_event(&mut self, _: LiveEvent) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_workflow() {
        init();
        System::run(move || {
            let addr = SyncArbiter::start(1, move || actor(Box::new(DummyStrat)));
            let order_book_event = LiveEventEnveloppe(
                Exchange::Binance,
                LiveEvent::LiveOrderbook(Orderbook {
                    timestamp: chrono::Utc::now().timestamp(),
                    pair: "BTC_USDT".into(),
                    asks: vec![(0.1, 0.1), (0.2, 0.2)],
                    bids: vec![(0.1, 0.1), (0.2, 0.2)],
                }),
            );
            println!("Sending...");
            for _ in 0..100000 {
                addr.do_send(order_book_event.clone());
            }
            thread::sleep(std::time::Duration::from_secs(2));
            for _ in 0..100000 {
                addr.do_send(order_book_event.clone());
            }
            thread::sleep(std::time::Duration::from_secs(2));
            for _ in 0..100000 {
                addr.do_send(order_book_event.clone());
            }
            System::current().stop();
        })
        .unwrap();
        assert_eq!(true, true);
    }
}
