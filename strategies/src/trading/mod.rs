pub mod interest;
pub mod stop;

/*use crate::order_types::Transaction;
use crate::types::{OperationKind, PositionKind, TradeOperation};
use std::marker::PhantomData;
use std::sync::Arc;
use uuid::Uuid;

struct PositionHolder<P> {
    pos: PhantomData<P>,
}

pub trait TradingPosition {
    fn kind(&self) -> PositionKind;

    fn trades(&self, opk: OperationKind, qty: f64, dry_mode: bool) -> Vec<TradeOperation>;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TradingOperation<P: TradingPosition> {
    pub id: String,
    pub kind: OperationKind,
    pub pos: P,
    pub transaction: Option<Transaction>,
    pub trades: Vec<TradeOperation>,
}

impl<P: TradingPosition + Clone> TradingOperation<P> {
    pub fn new(position: P, op_kind: OperationKind, qty: f64, dry_mode: bool) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            pos: position.clone(),
            kind: op_kind,
            transaction: None,
            trades: position.trades(op_kind, qty, dry_mode),
        }
    }

    fn is_open(&self) -> bool { matches!(self.kind, OperationKind::Open) }

    fn is_close(&self) -> bool { matches!(self.kind, OperationKind::Close) }
}

pub struct CrossOver<T: std::cmp::PartialOrd> {
    left: T,
    right: T,
}

impl<T: std::cmp::PartialOrd> CrossOver<T> {
    pub fn new(left: T, right: T) -> Self { Self { left, right } }
}

impl<T: std::cmp::PartialOrd> Signal for CrossOver<T> {
    fn check(&self) -> bool { (self.left)() > (self.right)() }
}

pub struct CrossUnder<T: std::cmp::PartialOrd> {
    left: T,
    right: T,
}

impl<T: std::cmp::PartialOrd> CrossUnder<T> {
    pub fn new(left: T, right: T) -> Self { Self { left, right } }
}

impl<T: std::cmp::PartialOrd> Signal for CrossUnder<T> {
    fn check(&self) -> bool { (self.left)() < (self.right)() }
}

trait Signal {
    fn check(&self) -> bool;
}

struct EnterExitSignal {
    enter: Arc<dyn Signal>,
    exit: Arc<dyn Signal>,
}

impl EnterExitSignal {
    pub fn new(enter: Arc<dyn Signal>, exit: Arc<dyn Signal>) -> Self { Self { enter, exit } }
    fn enter(&self) -> bool { self.enter.check() }

    fn exit(&self) -> bool { self.exit.check() }
}

#[async_trait]
trait Executor {
    async fn execute(&self, op_kind: OperationKind, pos_kind: PositionKind);
}

pub struct ShortLongTrader<P: TradingPosition, E: Executor> {
    pos: Option<PositionKind>,
    short_signal: EnterExitSignal,
    long_signal: EnterExitSignal,
    phantom: PhantomData<P>,
    executor: E,
}

impl<P: TradingPosition, E: Executor> ShortLongTrader<P, E> {
    pub fn new(short_signal: EnterExitSignal, long_signal: EnterExitSignal, executor: E) -> Self {
        Self {
            pos: None,
            short_signal,
            long_signal,
            phantom: Default::default(),
            executor,
        }
    }

    async fn potential_trade(&mut self) {
        if self.no_position_taken() {
            if self.short_signal.enter() {
                self.executor.execute(OperationKind::Open, PositionKind::Short).await
            } else if self.long_signal.enter() {
                self.executor.execute(OperationKind::Open, PositionKind::Long).await
            }
        } else if self.short_signal.exit() {
            self.executor.execute(OperationKind::Close, PositionKind::Short).await
        } else if self.long_signal.exit() {
            self.executor.execute(OperationKind::Close, PositionKind::Long).await
        }
    }

    fn no_position_taken(&self) -> bool { matches!(self.pos, None) }

    /*#[tracing::instrument(skip(self), level = "debug")]
    pub(super) async fn open(&mut self, pos: P) -> Result<TradingOperation<P>> {
        self.pos = pos.kind.clone();
        self.traded_price = pos.price;
        self.previous_value_strat = self.value_strat;
        self.update_nominal_position(&position_kind);
        self.update_open_value(self.value_strat, &position_kind, pos.price);
        let mut op = Operation::new(pos, OperationKind::Open, self.nominal_position, self.dry_mode);
        self.stage_operation(&mut op).await
    }*/
}
*/
