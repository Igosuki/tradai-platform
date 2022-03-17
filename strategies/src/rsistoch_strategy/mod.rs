use brokers::prelude::*;
use brokers::types::{Candle, SecurityType, Symbol};
use chrono::{DateTime, Utc};
use serde_json::Value;
use stats::indicators::{macd, rsi, stoch};
use stats::kline::{Resolution, TimeUnit};
use stats::yata_indicators::{StochasticOscillator, MACD, RSI};
use stats::yata_prelude::{IndicatorConfig, IndicatorInstance};
use stats::Action;
use stats::Source;
use std::collections::HashSet;
use strategy::driver::{DefaultStrategyContext, Strategy, TradeSignals};
use strategy::error::*;
use strategy::plugin::{provide_options, StrategyPlugin, StrategyPluginContext};
use strategy::settings::{StrategyOptions, StrategySettingsReplicator};
use strategy::{MarketChannel, MarketChannelType, StratEventLoggerRef, StrategyKey};
use trading::position::{OperationKind, PositionKind};
use trading::signal::{new_trade_signal, TradeSignal};
use trading::stop::TrailingStopper;
use trading::types::OrderConf;
use util::time::TimedData;
use uuid::Uuid;

#[cfg(all(test, feature = "backtests"))]
mod backtests;
#[cfg(any(test, feature = "backtests"))]
mod report;

pub fn provide_strat(_name: &str, ctx: StrategyPluginContext, conf: serde_json::Value) -> Result<Box<dyn Strategy>> {
    let options: Options = serde_json::from_value(conf)?;
    Ok(Box::new(StochRsiStrategy::try_new(&options, ctx.logger)?))
}

inventory::submit! {
    StrategyPlugin::new("stoch_rsi", provide_options::<Options>, provide_strat)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Options {
    exchange: Exchange,
    pair: Pair,
    source: Source,
    /// Len of RSI
    rsi_len: Option<u32>,
    /// Lower bound of rsi zone
    rsi_low: Option<f64>,
    //rsi_high: Option<f64>,
    /// Lower bound of stoch zone
    stoch_low: Option<f64>,
    //stoch_high: Option<f64>,
    /// Len of %K moving average
    stoch_len: Option<u32>,
    /// %K smoothing for the stoch oscillator
    smooth_k: Option<u32>,
    macd_fast: Option<u32>,
    macd_slow: Option<u32>,
    macd_signal: Option<u32>,
    resolution: Resolution,
    /// Trailing stopper trailing stop loss
    trailing_stop_loss: Option<f64>,
    /// Trailing stopper stop loss
    stop_loss: Option<f64>,
    /// Trailing stopper stop start
    trailing_stop_start: Option<f64>,
    /// Order Configuration to use for trade signals
    order_conf: OrderConf,
    /// Security Type to request for data
    security_type: SecurityType,
    /// Trade tick rate
    #[serde(
        deserialize_with = "util::ser::string_duration_chrono_opt",
        serialize_with = "util::ser::encode_duration_str_opt"
    )]
    tick_rate: Option<chrono::Duration>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            exchange: Exchange::Binance,
            pair: Default::default(),
            source: Source::Close,
            rsi_len: None,
            rsi_low: None,
            stoch_low: None,
            stoch_len: None,
            smooth_k: None,
            macd_fast: None,
            macd_slow: None,
            macd_signal: None,
            resolution: Resolution {
                time_unit: TimeUnit::MilliSecond,
                units: 0,
            },
            trailing_stop_loss: None,
            stop_loss: None,
            trailing_stop_start: None,
            order_conf: Default::default(),
            security_type: SecurityType::Crypto,
            tick_rate: None,
        }
    }
}

impl Options {
    fn validate(&self) -> Option<Error> {
        if self.rsi_len() < 1 {
            return Some(strategy::error::Error::BadConfiguration(
                "rsi_len should be > 1".to_string(),
            ));
        }
        None
    }

    fn rsi_len(&self) -> u32 { self.rsi_len.unwrap_or(14) }
    fn stoch_len(&self) -> u32 { self.stoch_len.unwrap_or(14) }
    fn rsi_low(&self) -> f64 { self.rsi_low.unwrap_or(0.35) }
    //fn rsi_high(&self) -> f64 { self.rsi_high.unwrap_or(65.0) }
    fn stoch_low(&self) -> f64 { self.stoch_low.unwrap_or(0.35) }
    //fn stoch_high(&self) -> f64 { self.stoch_high.unwrap_or(65.0) }
    fn smooth_k(&self) -> u32 { self.smooth_k.unwrap_or(1) }
    fn macd_fast(&self) -> u32 { self.macd_fast.unwrap_or(13) }
    fn macd_slow(&self) -> u32 { self.macd_slow.unwrap_or(21) }
    fn macd_signal(&self) -> u32 { self.macd_signal.unwrap_or(8) }
    fn stop_loss(&self) -> f64 { self.stop_loss.unwrap_or(-0.1) }
    fn trailing_stop_loss(&self) -> f64 { self.trailing_stop_loss.unwrap_or(0.02) }
    fn trailing_stop_start(&self) -> f64 { self.trailing_stop_start.unwrap_or(0.03) }
}

impl StrategySettingsReplicator for Options {
    fn replicate_for_pairs(&self, pairs: HashSet<Pair>) -> Vec<Value> {
        pairs
            .into_iter()
            .map(|pair| {
                let mut new = self.clone();
                new.pair = pair;
                serde_json::to_value(new).unwrap()
            })
            .collect()
    }
}

impl StrategyOptions for Options {
    fn key(&self) -> StrategyKey {
        StrategyKey(
            "rsistoch".to_string(),
            format!("rob_rsi_stoch_macd_{}_{}", self.exchange, self.pair),
        )
    }
}

#[derive(Serialize, Deserialize)]
struct SotchRsiValue {
    rsi: f64,
    stoch: f64,
    macd: f64,
}

/// Created by Robert Nance on 5/28/16. Additional credit to vdubus.
/// This was a special request from rich15stan.  It combines my original RSI Stoch extremes with vdubus’ MACD VXI.
/// This script will give you red or green columns as an indication for oversold/overbought,
/// based upon the rsi and stochastic both being at certain levels. The default oversold is at 35.
/// If Stochastic and RSI fall below 35, you will get a green column.  Play with your levels to see how
/// your stock reacts.  It now adds the MACD crossover, plotted as a blue circle.
pub struct StochRsiStrategy {
    exchange: Exchange,
    pair: Pair,
    stoch: StochasticOscillator,
    stoch_instance: Option<<StochasticOscillator as IndicatorConfig>::Instance>,
    macd: MACD,
    macd_instance: Option<<MACD as IndicatorConfig>::Instance>,
    rsi: RSI,
    rsi_instance: Option<<RSI as IndicatorConfig>::Instance>,
    main_signal: Option<Action>,
    last_macd_signal: Option<Action>,
    //kline: Kline,
    stopper: TrailingStopper<f64>,
    logger: Option<StratEventLoggerRef>,
    order_conf: OrderConf,
    value: Option<SotchRsiValue>,
    security_type: SecurityType,
    resolution: Resolution,
    tick_rate: Option<chrono::Duration>,
    rsi_low: f64,
    stoch_low: f64,
}

impl StochRsiStrategy {
    #[allow(clippy::cast_sign_loss)]
    pub fn try_new(n: &Options, logger: Option<StratEventLoggerRef>) -> Result<Self> {
        if let Some(err) = n.validate() {
            return Err(err);
        }
        let rsi = rsi(n.source, n.rsi_len(), n.rsi_low());
        let stoch = stoch(n.stoch_len(), n.smooth_k(), 3, n.stoch_low());
        let macd = macd(n.source, n.macd_fast(), n.macd_slow(), n.macd_signal());
        if !rsi.validate() || !stoch.validate() || !macd.validate() {
            return Err(Error::BadConfiguration("bad config".to_string()));
        }
        let strat = Self {
            rsi_low: n.rsi_low(),
            stoch_low: n.stoch_low(),
            exchange: n.exchange,
            pair: n.pair.clone(),
            rsi,
            stoch,
            stoch_instance: None,
            macd,
            rsi_instance: None,
            main_signal: None,
            macd_instance: None,
            //kline: Kline::new(n.resolution, 8),
            stopper: TrailingStopper::new(n.trailing_stop_start(), n.trailing_stop_loss(), n.stop_loss()),
            logger,
            order_conf: n.order_conf.clone(),
            value: None,
            last_macd_signal: None,
            security_type: n.security_type,
            resolution: n.resolution,
            tick_rate: n.tick_rate,
        };
        // TODO: temporary hack, use a report fn registry to allow for custom reports
        #[cfg(any(test, feature = "backtests"))]
        {
            let resolution = n.resolution.clone();
            backtest::report::register_report_fn(
                strat.key(),
                std::sync::Arc::new(move |report| {
                    report::edit_report(report, resolution);
                }),
            );
        }
        Ok(strat)
    }

    fn make_signal(
        &self,
        trace_id: Uuid,
        event_time: DateTime<Utc>,
        operation_kind: OperationKind,
        position_kind: PositionKind,
        price: f64,
        qty: Option<f64>,
    ) -> TradeSignal {
        new_trade_signal(
            self.pair.clone(),
            self.exchange,
            &self.order_conf,
            event_time,
            trace_id,
            operation_kind,
            position_kind,
            price,
            qty,
        )
    }

    async fn eval_candle<'a>(
        &mut self,
        le: &MarketEventEnvelope,
        broker_candle: &Candle,
        ctx: &DefaultStrategyContext<'_>,
    ) -> Result<Option<TradeSignals>> {
        let candle = stats::kline::Candle {
            event_time: broker_candle.event_time,
            start_time: broker_candle.start_time,
            end_time: broker_candle.end_time,
            open: broker_candle.open,
            high: broker_candle.high,
            low: broker_candle.low,
            close: broker_candle.close,
            volume: broker_candle.volume,
            quote_volume: broker_candle.quote_volume,
            trade_count: broker_candle.trade_count,
            is_final: broker_candle.is_final,
        };
        if candle.is_final {
            if self.macd_instance.is_none() || self.rsi_instance.is_none() || self.stoch_instance.is_none() {
                self.macd_instance = Some(self.macd.init(&candle).unwrap());
                self.rsi_instance = Some(self.rsi.init(&candle).unwrap());
                self.stoch_instance = Some(self.stoch.init(&candle).unwrap());
            } else if let (Some(macd), Some(rsi), Some(stoch)) = (
                self.macd_instance.as_mut(),
                self.rsi_instance.as_mut(),
                self.stoch_instance.as_mut(),
            ) {
                let macd_r = macd.next(&candle);
                self.last_macd_signal = Some(macd_r.signal(0));
                let rsi_r = rsi.next(&candle);
                let stoch_r = stoch.next(&candle);
                let rsi_value = rsi_r.value(0);
                let stoch_value = stoch_r.value(0);
                if rsi_value > 1. - self.rsi_low && stoch_value > 1. - self.stoch_low {
                    self.main_signal = Some(Action::BUY_ALL);
                } else if rsi_value < self.rsi_low && stoch_value < self.stoch_low {
                    self.main_signal = Some(Action::SELL_ALL);
                }
                self.value = Some(SotchRsiValue {
                    rsi: rsi_value,
                    stoch: stoch_value,
                    macd: macd_r.value(0),
                });
            }
        }
        let portfolio = ctx.portfolio;
        if !portfolio.has_any_open_position() {
            self.stopper.reset();
        }
        let signal = match portfolio.open_position(self.exchange, self.pair.clone()) {
            Some(pos) => {
                // TODO: move this logic to a single place in the code which can be reused
                let maybe_stop = self.stopper.should_stop(pos.unreal_profit_loss);
                if let Some(logger) = &self.logger {
                    if let Some(stop) = maybe_stop {
                        logger.log(TimedData::new(le.ts, stop.into())).await;
                    }
                }
                // Possibly close a short position
                if pos.is_short()
                    && (maybe_stop.is_some() || (candle.is_final && !matches!(self.main_signal, Some(Action::Sell(_)))))
                {
                    Some(self.make_signal(
                        le.trace_id,
                        le.ts,
                        OperationKind::Close,
                        PositionKind::Short,
                        candle.close,
                        None,
                    ))
                }
                // Possibly close a long position
                else if pos.is_long()
                    && (maybe_stop.is_some() || (candle.is_final && !matches!(self.main_signal, Some(Action::Buy(_)))))
                {
                    Some(self.make_signal(
                        le.trace_id,
                        le.ts,
                        OperationKind::Close,
                        PositionKind::Long,
                        candle.close,
                        None,
                    ))
                } else {
                    None
                }
            }
            None if matches!(self.main_signal, Some(Action::Sell(_))) => {
                // Possibly open a short position
                let qty = Some(portfolio.value() / candle.close);
                Some(self.make_signal(
                    le.trace_id,
                    le.ts,
                    OperationKind::Open,
                    PositionKind::Short,
                    candle.close,
                    qty,
                ))
            }
            None if matches!(self.main_signal, Some(Action::Buy(_))) => {
                // Possibly open a long position
                let qty = Some(portfolio.value() / candle.close);
                Some(self.make_signal(
                    le.trace_id,
                    le.ts,
                    OperationKind::Open,
                    PositionKind::Long,
                    candle.close,
                    qty,
                ))
            }
            _ => None,
        };
        return Ok(signal.map(|s| {
            let mut signals = TradeSignals::new();
            signals.push(s);
            signals
        }));
    }
}

#[async_trait]
impl Strategy for StochRsiStrategy {
    fn key(&self) -> String { format!("rob_rsi_stoch_macd_{}_{}", self.exchange, self.pair) }

    fn init(&mut self) -> Result<()> { Ok(()) }

    async fn eval(&mut self, le: &MarketEventEnvelope, ctx: &DefaultStrategyContext) -> Result<Option<TradeSignals>> {
        self.main_signal = None;
        let e = &le.e;
        match e {
            MarketEvent::TradeCandle(c) => {
                return self.eval_candle(le, c, ctx).await;
            }
            _ => Ok(None),
        }
    }

    fn model(&self) -> Vec<(String, Option<Value>)> {
        self.value
            .as_ref()
            .map(|v| {
                vec![
                    ("rsi".to_string(), serde_json::to_value(v.rsi).ok()),
                    ("stoch".to_string(), serde_json::to_value(v.stoch).ok()),
                    ("macd".to_string(), serde_json::to_value(v.macd).ok()),
                    (
                        "main_signal".to_string(),
                        self.main_signal.and_then(|s| serde_json::to_value(s.analog()).ok()),
                    ),
                    (
                        "macd_signal".to_string(),
                        self.last_macd_signal
                            .and_then(|s| serde_json::to_value(s.analog()).ok()),
                    ),
                ]
            })
            .unwrap_or_else(Vec::new)
    }

    fn channels(&self) -> HashSet<MarketChannel> {
        vec![MarketChannel::builder()
            .symbol(Symbol::new(self.pair.clone(), self.security_type, self.exchange))
            .r#type(MarketChannelType::Candles)
            .resolution(Some(self.resolution))
            .tick_rate(self.tick_rate)
            .build()]
        .into_iter()
        .collect()
    }
}
