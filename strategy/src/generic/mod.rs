use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::RwLock;

use coinnect_rt::prelude::*;
use db::Storage;
use portfolio::portfolio::{Portfolio, PortfolioRepoImpl};
use portfolio::risk::DefaultMarketRiskEvaluator;
use trading::engine::TradingEngine;
use trading::order_manager::types::StagedOrder;
use trading::position::Position;
use trading::signal::TradeSignal;

use crate::driver::{DefaultStrategyContext, Strategy, StrategyDriver};
use crate::error::Result;
use crate::query::{DataQuery, DataResult, ModelReset, MutableField, Mutation, PortfolioSnapshot};
use crate::{Channel, StrategyStatus};

mod metrics;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PortfolioOptions {
    pub initial_quote_cash: f64,
    // TODO: replace by getting it from the exchange conf
    pub fees_rate: f64,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct GenericDriverOptions {
    pub portfolio: PortfolioOptions,
    /// Start trading after first start
    pub start_trading: Option<bool>,
    pub dry_mode: Option<bool>,
}

impl GenericDriverOptions {
    pub fn dry_mode(&self) -> bool { self.dry_mode.unwrap_or(false) }
}

pub struct GenericDriver {
    channels: HashSet<Channel>,
    pub(crate) inner: RwLock<Box<dyn Strategy>>,
    initialized: bool,
    is_trading: bool,
    pub(crate) portfolio: Portfolio,
    engine: Arc<TradingEngine>,
    strat_key: String,
    last_event: Option<MarketEventEnvelope>,
}

impl GenericDriver {
    pub fn try_new(
        channels: HashSet<Channel>,
        db: Arc<dyn Storage>,
        driver_options: &GenericDriverOptions,
        strat: Box<dyn Strategy>,
        engine: Arc<TradingEngine>,
    ) -> Result<Self> {
        let portfolio_options = &driver_options.portfolio;
        let strat_key = strat.key();
        let portfolio = Portfolio::try_new(
            portfolio_options.initial_quote_cash,
            portfolio_options.fees_rate,
            strat_key.clone(),
            Arc::new(PortfolioRepoImpl::new(db)),
            Arc::new(DefaultMarketRiskEvaluator::default()),
            engine.interest_rate_provider.clone(),
        )?;
        Ok(Self {
            channels,
            inner: RwLock::new(strat),
            initialized: false,
            is_trading: driver_options.start_trading.unwrap_or(true),
            portfolio,
            engine,
            strat_key,
            last_event: None,
        })
    }

    async fn init(&self) -> Result<()> {
        let mut strat = self.inner.write().await;
        strat.init()
    }

    fn handles(&self, le: &MarketEventEnvelope) -> bool {
        // TODO : replace this with a hashtable
        self.channels.iter().any(|c| match (c, le) {
            (
                Channel::Orderbooks { pair, xch },
                le @ MarketEventEnvelope {
                    e: MarketEvent::Orderbook(ob),
                    ..
                },
            ) => pair == &ob.pair && xch == &le.xch,
            _ => false,
        })
    }

    pub(crate) fn status(&self) -> StrategyStatus {
        if self.is_trading {
            StrategyStatus::Running
        } else {
            StrategyStatus::NotTrading
        }
    }

    async fn process_signals(&mut self, signals: &[TradeSignal]) -> Result<()> {
        metrics::get().log_signals(self.strat_key.as_str(), signals);
        let mut orders = vec![];
        for signal in signals {
            let conversion = self.portfolio.maybe_convert(signal).await;
            match conversion {
                Ok(Some(order)) => orders.push(order),
                Err(e) => error!(err = %e, "failed to convert order"),
                _ => error!(signal = ?signal, "did not convert to an order"),
            }
        }
        if orders.len() != signals.len() {
            return Ok(());
        }
        for order in orders {
            let exchange = order.xch;
            let pair = order.pair.clone();
            if let Err(e) = self
                .engine
                .order_executor
                .stage_order(StagedOrder { request: order })
                .await
            {
                // TODO : keep result and immediatly try to close (or retry) failed orders
                metrics::get().log_error(e.short_name());
                error!(err = %e, "failed to stage order");
                if let Err(e) = self.portfolio.unlock_position(exchange, pair) {
                    metrics::get().log_error(e.short_name());
                    error!(err = %e, "failed to unlock position");
                }
            }
        }
        metrics::get().log_portfolio(self.strat_key.as_str(), &self.portfolio);
        Ok(())
    }

    fn indicators(&self) -> PortfolioSnapshot {
        PortfolioSnapshot {
            value: self.portfolio.value(),
            pnl: self.portfolio.pnl(),
            current_return: self.portfolio.current_return(),
        }
    }

    async fn process_event(&mut self, le: &MarketEventEnvelope) -> Result<()> {
        if !self.initialized {
            self.init().await.unwrap();
            self.initialized = true;
        }
        if let Err(e) = self.portfolio.update_from_market(le).await {
            metrics::get().log_error(e.short_name());
            error!(err = %e, "failed to update portfolio from market");
        }
        let signals = {
            let mut inner = self.inner.write().await;
            inner.eval(le, &self.ctx()).await?
        };
        metrics::get().log_is_trading(self.strat_key.as_str(), self.is_trading);
        if self.portfolio.has_any_failed_position() {
            metrics::get().log_failed_position(&le.xch, &le.pair);
            return Ok(());
        }
        if !self.portfolio.locks().is_empty() {
            metrics::get().log_lock(&le.xch, &le.pair);
            return Ok(());
        }
        if self.is_trading {
            if let Some(signals) = signals {
                if !signals.is_empty() {
                    if let Err(e) = self.process_signals(signals.as_slice()).await {
                        metrics::get().signal_error(&le.xch, &le.pair);
                        metrics::get().log_error(e.short_name());
                        error!(err = %e, "error processing signals");
                    }
                }
            }
        }
        Ok(())
    }

    pub fn ctx(&self) -> DefaultStrategyContext {
        DefaultStrategyContext {
            portfolio: &self.portfolio,
        }
    }
}

#[async_trait]
impl StrategyDriver for GenericDriver {
    async fn key(&self) -> String {
        let r = self.inner.read().await;
        r.key()
    }

    async fn add_event(&mut self, le: &MarketEventEnvelope) -> Result<()> {
        if !self.handles(le) {
            return Ok(());
        }
        self.last_event = Some(le.clone());
        self.process_event(le).await.map_err(|e| {
            metrics::get().log_error(e.short_name());
            e
        })
    }

    async fn data(&mut self, q: DataQuery) -> Result<DataResult> {
        match q {
            DataQuery::CancelOngoingOp => Ok(DataResult::Success(false)),
            DataQuery::Models => {
                let inner = self.inner.read().await;
                Ok(DataResult::Models(inner.model()))
            }
            DataQuery::Status => Ok(DataResult::Status(self.status())),
            DataQuery::Indicators => Ok(DataResult::Indicators(self.indicators())),
            DataQuery::PositionHistory => Ok(DataResult::PositionHistory(self.portfolio.positions_history()?)),
            DataQuery::OpenPositions => Ok(DataResult::OpenPositions(
                self.portfolio
                    .open_positions()
                    .values()
                    .cloned()
                    .collect::<Vec<Position>>(),
            )),
        }
    }

    fn mutate(&mut self, m: Mutation) -> Result<()> {
        match m {
            Mutation::State(m) => {
                match m.field {
                    MutableField::ValueStrat => self.portfolio.set_value(m.value)?,
                    MutableField::Pnl => self.portfolio.set_pnl(m.value)?,
                }
                Ok(())
            }
            Mutation::Model(ModelReset { name: _, .. }) => {
                unimplemented!()
            }
        }
    }

    fn channels(&self) -> HashSet<Channel> { self.channels.clone() }

    fn stop_trading(&mut self) { self.is_trading = false; }

    fn resume_trading(&mut self) { self.is_trading = true; }

    async fn resolve_orders(&mut self) {
        if self.portfolio.locks().is_empty() {
            return;
        }
        // TODO : bad performance overall
        let locked_ids: Vec<String> = self.portfolio.locks().values().map(|v| v.order_id.clone()).collect();
        for lock in &locked_ids {
            match self.engine.order_executor.get_order(lock.as_str()).await {
                Ok((order, _)) => {
                    if let Err(e) = self.portfolio.update_position(order) {
                        metrics::get().log_error(e.short_name());
                        error!(err = %e, "failed to update portfolio position");
                    }
                }
                Err(e) => {
                    metrics::get().log_error(e.short_name());
                    error!(err = %e, "failed to query locked order");
                }
            }
        }
        if !locked_ids.is_empty() && self.portfolio.locks().is_empty() {
            let mut inner_w = self.inner.write().await;
            if let Some(event) = self.last_event.as_ref() {
                if let Err(e) = inner_w.eval(event, &self.ctx()).await {
                    metrics::get().log_error(e.short_name());
                    error!(err = %e, "failed to eval after unlocking portfolio");
                }
            }
        }
    }

    async fn is_locked(&self) -> bool { !self.portfolio.locks().is_empty() }
}
