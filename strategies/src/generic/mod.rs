use std::collections::{BTreeMap, HashSet};
use std::convert::TryInto;
use std::sync::{Arc, Mutex};

use tokio::sync::RwLock;

use coinnect_rt::prelude::*;

use crate::driver::StrategyDriver;
use crate::error::Result;
use crate::query::{DataQuery, DataResult, Mutation, StrategyIndicators};
use crate::types::InputEvent;
use crate::{Channel, StrategyStatus};
use trading::book::BookPosition;
use trading::signal::TradeSignal;

#[async_trait]
pub(crate) trait Strategy: Sync + Send {
    //async fn try_new(&self, conf: serde_json::Value) -> Self;

    fn key(&self) -> String;

    fn init(&mut self) -> Result<()>;

    async fn eval(&mut self, e: &InputEvent) -> Result<Vec<TradeSignal>>;

    async fn update_model(&mut self, e: &InputEvent) -> Result<()>;

    fn models(&self) -> Vec<(String, Option<serde_json::Value>)>;

    fn channels(&self) -> HashSet<Channel>;

    fn indicators(&self) -> StrategyIndicators;
}

#[allow(dead_code)]
struct StrategyContext<C> {
    db: Arc<dyn Strategy>,
    conf: C,
}

pub struct GenericStrategy {
    channels: HashSet<Channel>,
    last_positions: Mutex<BTreeMap<Pair, BookPosition>>,
    inner: RwLock<Box<dyn Strategy>>,
    multi_market: bool,
    initialized: bool,
    signals: Vec<TradeSignal>,
    last_models: Vec<(String, Option<serde_json::Value>)>,
    last_indicators: StrategyIndicators,
    is_trading: bool,
}

impl GenericStrategy {
    pub(crate) fn try_new(channels: HashSet<Channel>, strat: Box<dyn Strategy>) -> Result<Self> {
        Ok(Self {
            channels,
            last_positions: Mutex::new(Default::default()),
            inner: RwLock::new(strat),
            multi_market: false,
            initialized: false,
            signals: Default::default(),
            last_models: vec![],
            last_indicators: StrategyIndicators::default(),
            is_trading: true,
        })
    }

    async fn init(&self) -> Result<()> {
        let mut strat = self.inner.write().await;
        strat.init()
    }

    fn handles(&self, le: &LiveEventEnvelope) -> bool {
        self.channels.iter().any(|c| match (c, le) {
            (
                Channel::Orderbooks { pair, xch },
                le @ LiveEventEnvelope {
                    e: MarketEvent::Orderbook(ob),
                    ..
                },
            ) => pair == &ob.pair && xch == &le.xch,
            _ => false,
        })
    }
    //
    // async fn get_operations(&self) -> Vec<TradeOperation>;
    //
    // async fn cancel_ongoing_operations(&self) -> bool;
    //
    // async fn get_state(&self) -> String;
    //
    // async fn get_status(&self) -> StrategyStatus;
}

#[async_trait]
impl StrategyDriver for GenericStrategy {
    async fn key(&self) -> String {
        let r = self.inner.read().await;
        r.key()
    }

    async fn add_event(&mut self, le: &LiveEventEnvelope) -> Result<()> {
        if !self.initialized {
            self.init().await.unwrap();
            self.initialized = true;
        }
        if !self.handles(le) {
            return Ok(());
        }
        if let MarketEvent::Orderbook(ob) = &le.e {
            if let Ok(pos) = ob.try_into() {
                let event = if self.multi_market {
                    let positions = {
                        let mut lock = self.last_positions.lock().unwrap();
                        lock.insert(ob.pair.clone(), pos);
                        lock.clone()
                    };
                    InputEvent::BookPositions(positions)
                } else {
                    InputEvent::BookPosition(pos)
                };
                {
                    let mut inner = self.inner.write().await;
                    if inner.update_model(&event).await.is_ok() {
                        self.last_models = inner.models();
                    }
                    let signals = inner.eval(&event).await.unwrap();
                    self.signals.extend(signals);
                    self.last_indicators = inner.indicators();
                }
            }
        }
        Ok(())
    }

    fn data(&mut self, q: DataQuery) -> Result<DataResult> {
        match q {
            DataQuery::OperationHistory => Ok(DataResult::Operations(vec![])),
            DataQuery::OpenOperations => Ok(DataResult::Operations(vec![])),
            DataQuery::CancelOngoingOp => Ok(DataResult::Success(false)),
            DataQuery::State => Ok(DataResult::State("".to_string())),
            DataQuery::Models => Ok(DataResult::Models(self.last_models.to_owned())),
            DataQuery::Status => Ok(DataResult::Status(StrategyStatus::NotTrading)),
            DataQuery::Indicators => Ok(DataResult::Indicators(self.last_indicators.clone())),
        }
    }

    fn mutate(&mut self, _m: Mutation) -> Result<()> { Ok(()) }

    fn channels(&self) -> Vec<Channel> {
        // self.exchanges
        //     .iter()
        //     .map(|xchg| {
        //         self.pairs.iter().map(move |pair| Channel::Orderbooks {
        //             xch: *xchg,
        //             pair: pair.clone(),
        //         })
        //     })
        //     .flatten()
        //     .collect()
        self.channels.clone().into_iter().collect()
    }

    fn stop_trading(&mut self) { self.is_trading = false; }

    fn resume_trading(&mut self) { self.is_trading = true; }

    async fn resolve_orders(&mut self) { todo!() }
}
