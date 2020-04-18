use actix::{Actor, Handler, Running, SyncContext};
use coinnect_rt::types::{LiveEvent, LiveEventEnveloppe};
use derive_more::Display;
use std::sync::Arc;
use uuid::Uuid;

pub mod naive_pair_trading;

pub struct StrategyActorOptions<S: StrategySink> {
    strategy: S,
}

#[derive(Debug, Display)]
pub enum Error {
    IOError(std::io::Error),
}

impl std::error::Error for Error {}

pub struct StrategyActor<S: StrategySink> {
    _session_uuid: Uuid,
    inner: Arc<S>,
}

impl<S: StrategySink> StrategyActor<S> {
    pub fn new(options: StrategyActorOptions<S>) -> Self {
        Self {
            _session_uuid: Uuid::new_v4(),
            inner: Arc::new(options.strategy),
        }
    }
}

impl<S: 'static + StrategySink + Unpin> Actor for StrategyActor<S> {
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

impl<S: 'static + StrategySink + Unpin> Handler<LiveEventEnveloppe> for StrategyActor<S> {
    type Result = ();

    #[cfg_attr(feature = "flame_it", flame)]
    fn handle(&mut self, msg: LiveEventEnveloppe, _ctx: &mut Self::Context) -> Self::Result {
        self.inner.add_event(msg.1).unwrap();
    }
}

pub trait StrategySink {
    fn add_event(&self, le: LiveEvent) -> std::io::Result<()>;
}

#[cfg(test)]
mod test {
    use std::thread;

    use actix::SyncArbiter;
    use actix_rt::System;
    use bigdecimal::BigDecimal;
    use coinnect_rt::exchange::Exchange;
    use coinnect_rt::types::Orderbook;
    use coinnect_rt::types::Pair;

    use super::*;

    #[allow(dead_code)]
    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    fn actor<S: StrategySink>(strategy: S) -> StrategyActor<S> {
        StrategyActor::new(StrategyActorOptions { strategy })
    }

    struct DummyStrat;
    impl StrategySink for DummyStrat {
        fn add_event(&self, _: LiveEvent) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_workflow() {
        System::run(move || {
            let addr = SyncArbiter::start(1, move || actor(DummyStrat));
            let order_book_event = LiveEventEnveloppe(
                Exchange::Binance,
                LiveEvent::LiveOrderbook(Orderbook {
                    timestamp: chrono::Utc::now().timestamp(),
                    pair: Pair::BTC_USDT,
                    asks: vec![
                        (BigDecimal::from(0.1), BigDecimal::from(0.1)),
                        (BigDecimal::from(0.2), BigDecimal::from(0.2)),
                    ],
                    bids: vec![
                        (BigDecimal::from(0.1), BigDecimal::from(0.1)),
                        (BigDecimal::from(0.2), BigDecimal::from(0.2)),
                    ],
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
