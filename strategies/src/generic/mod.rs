mod metrics;

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::RwLock;

use coinnect_rt::prelude::*;
use db::Storage;
use portfolio::portfolio::{Portfolio, PortfolioRepoImpl};
use portfolio::risk::DefaultMarketRiskEvaluator;
use trading::engine::TradingEngine;

use crate::driver::StrategyDriver;
use crate::error::Result;
use crate::query::{DataQuery, DataResult, Mutation, StrategyIndicators};
use crate::{Channel, StrategyStatus};
use trading::order_manager::types::StagedOrder;
use trading::signal::TradeSignal;

#[async_trait]
pub(crate) trait Strategy: Sync + Send {
    //async fn try_new(&self, conf: serde_json::Value) -> Self;

    fn key(&self) -> String;

    fn init(&mut self) -> Result<()>;

    async fn eval(&mut self, e: &MarketEventEnvelope) -> Result<Vec<TradeSignal>>;

    async fn update_model(&mut self, e: &MarketEventEnvelope) -> Result<()>;

    fn model(&self) -> Vec<(String, Option<serde_json::Value>)>;

    fn channels(&self) -> HashSet<Channel>;
}

#[allow(dead_code)]
struct StrategyContext<C> {
    db: Arc<dyn Strategy>,
    conf: C,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PortfolioOptions {
    initial_quote_cash: f64,
    fees_rate: f64,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct GenericDriverOptions {
    portfolio: PortfolioOptions,
}

pub struct GenericDriver {
    channels: HashSet<Channel>,
    inner: RwLock<Box<dyn Strategy>>,
    initialized: bool,
    is_trading: bool,
    portfolio: Portfolio,
    engine: Arc<TradingEngine>,
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
        let portfolio = Portfolio::try_new(
            portfolio_options.initial_quote_cash,
            portfolio_options.fees_rate,
            strat.key(),
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
    // async fn get_status(&self) -> StrategyStatus;
    async fn process_signals(&mut self, signals: &[TradeSignal]) -> Result<()> {
        for signal in signals {
            let order = self.portfolio.maybe_convert(signal)?;
            if let Some(order) = order {
                self.engine
                    .order_executor
                    .stage_order(StagedOrder { request: order })
                    .await?;
            }
        }
        Ok(())
    }

    fn indicators(&self) -> StrategyIndicators {
        StrategyIndicators {
            value: self.portfolio.value(),
            pnl: self.portfolio.pnl(),
            current_return: self.portfolio.returns().last().map(|l| l.1).unwrap_or(0.0),
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
            inner.eval(le).await?
        };
        if !self.portfolio.is_locked(&(le.xch, le.pair.clone())) && !self.portfolio.has_any_failed_position() {
            if let Err(_e) = self.process_signals(signals.as_slice()).await {
                metrics::get().signal_error(&le.xch, &le.pair);
            }
        } else {
            metrics::get().log_lock(&le.xch, &le.pair)
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

    fn data(&mut self, q: DataQuery) -> Result<DataResult> {
        match q {
            DataQuery::OperationHistory => Ok(DataResult::Operations(vec![])),
            DataQuery::OpenOperations => Ok(DataResult::Operations(vec![])),
            DataQuery::CancelOngoingOp => Ok(DataResult::Success(false)),
            DataQuery::State => Ok(DataResult::State("".to_string())),
            //self.inner.read().await.model().to_owned()
            DataQuery::Models => Ok(DataResult::Models(vec![])),
            DataQuery::Status => Ok(DataResult::Status(StrategyStatus::NotTrading)),
            DataQuery::Indicators => Ok(DataResult::Indicators(self.indicators())),
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
        let locked_ids: Vec<String> = self.portfolio.locks().iter().map(|(k, v)| v.order_id.clone()).collect();
        for lock in locked_ids {
            match self.engine.order_executor.get_order(lock.as_str()).await {
                Ok((order, _)) => {
                    if let Err(_e) = self.portfolio.update_position(order) {
                        // log err
                    }
                }
                Err(_e) => {
                    //log err
                }
            }
        }
    }
}
