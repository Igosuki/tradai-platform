use crate::error::Result;
use crate::query::{DataQuery, DataResult, FieldMutation};
use crate::types::{BookPosition, ExecutionInstruction, OperationKind, PositionKind, TradeKind};
use crate::{Channel, StrategyDriver, StrategyStatus};
use coinnect_rt::exchange::Exchange;
use coinnect_rt::types::{AssetType, LiveEvent, LiveEventEnvelope, Pair};
use std::collections::{BTreeMap, HashSet};
use std::convert::TryInto;
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

#[async_trait]
pub(crate) trait Strategy: Sync + Send {
    //async fn try_new(&self, conf: serde_json::Value) -> Self;

    fn init(&mut self) -> Result<()>;

    async fn eval(&mut self, e: &InputEvent) -> Result<Vec<TradeSignal>>;

    async fn update_model(&mut self, e: &InputEvent) -> Result<()>;

    fn models(&self) -> Vec<(&str, Option<serde_json::Value>)>;

    fn channels(&self) -> HashSet<Channel>;
}

type BookPositions = BTreeMap<Pair, BookPosition>;

#[allow(dead_code)]
struct StrategyContext<C> {
    db: Arc<dyn Strategy>,
    conf: C,
}

pub(crate) enum InputEvent {
    BookPosition(BookPosition),
    BookPositions(BookPositions),
}

#[derive(Debug)]
pub(crate) struct TradeSignal {
    pub position_kind: PositionKind,
    pub operation_kind: OperationKind,
    pub trade_kind: TradeKind,
    pub price: f64,
    pub pair: Pair,
    pub exchange: Exchange,
    pub instructions: Option<ExecutionInstruction>,
    pub dry_mode: bool,
    pub asset_type: AssetType,
}

pub struct GenericStrategy {
    channels: HashSet<Channel>,
    last_positions: Mutex<BTreeMap<Pair, BookPosition>>,
    inner: RwLock<Box<dyn Strategy>>,
    multi_market: bool,
    initialized: bool,
    signals: Vec<TradeSignal>,
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
                le
                @
                LiveEventEnvelope {
                    e: LiveEvent::LiveOrderbook(ob),
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
    async fn add_event(&mut self, le: &LiveEventEnvelope) -> Result<()> {
        if !self.initialized {
            self.init().await.unwrap();
            self.initialized = true;
        }
        if !self.handles(le) {
            return Ok(());
        }
        if let LiveEvent::LiveOrderbook(ob) = &le.e {
            let ob_pair = ob.pair.clone();
            let book_pos = ob.try_into().ok();
            if let Some(pos) = book_pos {
                let event = if self.multi_market {
                    let positions = {
                        let mut lock = self.last_positions.lock().unwrap();
                        lock.insert(ob_pair, pos);
                        lock.clone()
                    };
                    InputEvent::BookPositions(positions)
                } else {
                    InputEvent::BookPosition(pos)
                };
                {
                    let mut inner = self.inner.write().await;
                    inner.update_model(&event).await.unwrap();
                    let signals = inner.eval(&event).await.unwrap();
                    self.signals.extend(signals);
                }
            }
        }
        Ok(())
    }

    fn data(&mut self, q: DataQuery) -> Option<DataResult> {
        match q {
            DataQuery::OperationHistory => Some(DataResult::Operations(vec![])),
            DataQuery::OpenOperations => Some(DataResult::Operations(vec![])),
            DataQuery::CancelOngoingOp => Some(DataResult::OperationCanceled(false)),
            DataQuery::State => Some(DataResult::State("".to_string())),
            DataQuery::Status => Some(DataResult::Status(StrategyStatus::NotTrading)),
        }
    }

    fn mutate(&mut self, _m: FieldMutation) -> Result<()> { Ok(()) }

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
}
