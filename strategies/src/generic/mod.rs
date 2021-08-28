use crate::error::Result;
use crate::query::{DataQuery, DataResult, FieldMutation};
use crate::types::{BookPosition, ExecutionInstruction, OperationKind, PositionKind, TradeKind};
use crate::{Channel, StrategyDriver, StrategyStatus};
use coinnect_rt::exchange::Exchange;
use coinnect_rt::types::{AssetType, LiveEvent, LiveEventEnvelope, Pair};
use std::collections::{BTreeMap, HashSet};
use std::convert::TryInto;
use std::sync::{Arc, Mutex};

struct StrategyContext<C> {
    db: Arc<dyn Strategy>,
    conf: C,
}

enum InputEvent {
    BookPosition(BookPosition),
    BookPositions(BookPositions),
}

#[async_trait]
trait Strategy: Sync + Send {
    async fn eval(&self, e: InputEvent) -> Result<Vec<TradeSignal>>;

    async fn update_model(&self, e: InputEvent) -> Result<()>;

    async fn get_models(&self);
}

//async fn try_new(&self, conf: serde_json::Value) -> Self;

type BookPositions = BTreeMap<Pair, BookPosition>;

struct TradeSignal {
    position_kind: PositionKind,
    operation_kind: OperationKind,
    trade_kind: TradeKind,
    price: f64,
    pair: Pair,
    exchange: Exchange,
    instructions: Option<ExecutionInstruction>,
    dry_mode: bool,
    asset_type: AssetType,
}

struct GenericStrategy {
    pairs: HashSet<Pair>,
    exchanges: HashSet<Exchange>,
    last_positions: Mutex<BTreeMap<Pair, BookPosition>>,
    inner: Arc<dyn Strategy>,
    multi_market: bool,
}

impl GenericStrategy {
    fn init(&self) {
        // get_models
        // load_models
    }
    fn handles(&self, le: &LiveEventEnvelope) -> bool {
        self.exchanges.contains(&le.xch)
            && match &le.e {
                LiveEvent::LiveOrderbook(ob) => self.pairs.contains(&ob.pair),
                _ => false,
            }
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
                self.inner.update_model(event).await.unwrap();
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

    fn mutate(&mut self, m: FieldMutation) -> Result<()> { Ok(()) }

    fn channels(&self) -> Vec<Channel> {
        self.exchanges
            .iter()
            .map(|xchg| {
                self.pairs.iter().map(move |pair| Channel::Orderbooks {
                    xch: *xchg,
                    pair: pair.clone(),
                })
            })
            .flatten()
            .collect()
    }
}
