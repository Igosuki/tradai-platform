use std::collections::HashSet;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::RwLock;

use coinnect_rt::prelude::*;
use db::Storage;
use portfolio::portfolio::{Portfolio, PortfolioRepoImpl};
use portfolio::risk::DefaultMarketRiskEvaluator;
use trading::engine::TradingEngine;
use trading::order_manager::types::StagedOrder;
use trading::signal::TradeSignal;

use crate::driver::StrategyDriver;
use crate::error::Result;
use crate::query::{DataQuery, DataResult, Mutation, StrategyIndicators};
use crate::{Channel, StrategyStatus};

mod metrics;

pub type SerializedModel = Vec<(String, Option<Value>)>;

#[async_trait]
pub(crate) trait Strategy: Sync + Send {
    //async fn try_new(&self, conf: serde_json::Value) -> Self;

    fn key(&self) -> String;

    fn init(&mut self) -> Result<()>;

    async fn eval(&mut self, e: &MarketEventEnvelope) -> Result<Option<Vec<TradeSignal>>>;

    async fn update_model(&mut self, e: &MarketEventEnvelope) -> Result<()>;

    fn model(&self) -> SerializedModel;

    fn channels(&self) -> HashSet<Channel>;
}

#[allow(dead_code)]
struct StrategyContext<C> {
    db: Arc<dyn Strategy>,
    conf: C,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PortfolioOptions {
    pub initial_quote_cash: f64,
    pub fees_rate: f64,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct GenericDriverOptions {
    pub portfolio: PortfolioOptions,
}

pub struct GenericDriver {
    channels: HashSet<Channel>,
    pub(crate) inner: RwLock<Box<dyn Strategy>>,
    initialized: bool,
    is_trading: bool,
    pub(crate) portfolio: Portfolio,
    engine: Arc<TradingEngine>,
    strat_key: String,
}

impl GenericDriver {
    pub(crate) fn try_new(
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
            is_trading: true,
            portfolio,
            engine,
            strat_key,
        })
    }

    async fn init(&self) -> Result<()> {
        let mut strat = self.inner.write().await;
        strat.init()
    }

    fn handles(&self, le: &MarketEventEnvelope) -> bool {
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
    //
    // async fn get_operations(&self) -> Vec<TradeOperation>;
    //
    // async fn cancel_ongoing_operations(&self) -> bool;
    //
    // async fn get_state(&self) -> String;
    //
    pub(crate) fn status(&self) -> StrategyStatus {
        if self.is_trading {
            StrategyStatus::Running
        } else {
            StrategyStatus::NotTrading
        }
    }

    async fn process_signals(&mut self, signals: &[TradeSignal]) -> Result<()> {
        metrics::get().log_signals(self.strat_key.as_str(), signals);
        for signal in signals {
            let order = self.portfolio.maybe_convert(signal).await?;
            if let Some(order) = order {
                self.engine
                    .order_executor
                    .stage_order(StagedOrder { request: order })
                    .await?;
            }
        }
        let mut orders = vec![];
        for signal in signals {
            let order = self.portfolio.maybe_convert(signal).await;
            if let Ok(Some(order)) = order {
                orders.push(order);
            }
        }
        if orders.len() != 2 {
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

    fn indicators(&self) -> StrategyIndicators {
        StrategyIndicators {
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
        if !self.handles(le) {
            return Ok(());
        }
        let signals = {
            let mut inner = self.inner.write().await;
            inner.update_model(le).await?;
            if let Err(e) = self.portfolio.update_from_market(le).await {
                // TODO: in metrics
                error!(err = %e, "failed to update portfolio from market");
            }
            inner.eval(le).await?
        };
        if let Some(signals) = signals {
            if !self.portfolio.is_locked(&(le.xch, le.pair.clone())) && !self.portfolio.has_any_failed_position() {
                if let Err(e) = self.process_signals(signals.as_slice()).await {
                    metrics::get().signal_error(&le.xch, &le.pair);
                    metrics::get().log_error(e.short_name());
                }
            } else {
                metrics::get().log_lock(&le.xch, &le.pair)
            }
        }
        Ok(())
    }
}

#[async_trait]
impl StrategyDriver for GenericDriver {
    async fn key(&self) -> String {
        let r = self.inner.read().await;
        r.key()
    }

    async fn add_event(&mut self, le: &MarketEventEnvelope) -> Result<()> {
        self.process_event(le).await.map_err(|e| {
            metrics::get().log_error(e.short_name());
            e
        })
    }

    async fn data(&mut self, q: DataQuery) -> Result<DataResult> {
        match q {
            DataQuery::CancelOngoingOp => Ok(DataResult::Success(false)),
            //self.inner.read().await.model().to_owned()
            DataQuery::Models => {
                let inner = self.inner.read().await;
                Ok(DataResult::Models(inner.model()))
            }
            DataQuery::Status => Ok(DataResult::Status(self.status())),
            DataQuery::Indicators => Ok(DataResult::Indicators(self.indicators())),
            DataQuery::PositionHistory => {
                unimplemented!()
            }
            DataQuery::OpenPositions => {
                unimplemented!()
            }
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

    async fn resolve_orders(&mut self) {
        // TODO : probably bad performance
        let locked_ids: Vec<String> = self
            .portfolio
            .locks()
            .iter()
            .map(|(_k, v)| v.order_id.clone())
            .collect();
        for lock in &locked_ids {
            match self.engine.order_executor.get_order(lock.as_str()).await {
                Ok((order, _)) => {
                    if let Err(e) = self.portfolio.update_position(order) {
                        // TODO: in metrics
                        error!(err = %e, "failed to update portfolio position");
                    }
                }
                Err(e) => {
                    // TODO: in metrics
                    error!(err = %e, "failed to query locked order");
                }
            }
        }
        if !locked_ids.is_empty() && self.portfolio.locks().is_empty() {
            // if let Err(e) = self.inner.eval_after_unlock().await {
            //     // TODO: in metrics
            //     error!(err = %e, "failed to eval after unlocking portfolio");
            // }
        }
    }
}
