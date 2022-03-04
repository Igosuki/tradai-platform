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
use std::sync::Arc;
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
    trailing_stop_loss: Option<f64>,
    stop_loss: Option<f64>,
    trailing_stop_start: Option<f64>,
    order_conf: OrderConf,
    security_type: SecurityType,
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
        };
        // TODO: temporary hack, use a report fn registry to allow for custom reports
        #[cfg(feature = "backtests")]
        {
            let resolution = n.resolution.clone();
            backtest::report::register_report_fn(
                strat.key(),
                Arc::new(move |report| {
                    report::edit_report(report, resolution);
                }),
            );
        }
        eprintln!("reported = {:?}", true);
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
        final_candle: &Candle,
        ctx: &DefaultStrategyContext<'_>,
    ) -> Result<Option<TradeSignals>> {
        let final_candle = stats::kline::Candle {
            event_time: final_candle.event_time,
            start_time: final_candle.start_time,
            end_time: final_candle.end_time,
            open: final_candle.open,
            high: final_candle.high,
            low: final_candle.low,
            close: final_candle.close,
            volume: final_candle.volume,
            quote_volume: final_candle.quote_volume,
            trade_count: final_candle.trade_count,
            is_final: final_candle.is_final,
        };

        let portfolio = ctx.portfolio;
        if !portfolio.has_any_open_position() {
            self.stopper.reset();
        }
        if self.macd_instance.is_none() || self.rsi_instance.is_none() || self.stoch_instance.is_none() {
            self.macd_instance = Some(self.macd.init(&final_candle).unwrap());
            self.rsi_instance = Some(self.rsi.init(&final_candle).unwrap());
            self.stoch_instance = Some(self.stoch.init(&final_candle).unwrap());
        } else if let (Some(macd), Some(rsi), Some(stoch)) = (
            self.macd_instance.as_mut(),
            self.rsi_instance.as_mut(),
            self.stoch_instance.as_mut(),
        ) {
            let macd_r = macd.next(&final_candle);
            self.last_macd_signal = Some(macd_r.signal(0));
            let rsi_r = rsi.next(&final_candle);
            let stoch_r = stoch.next(&final_candle);
            let rsi_value = rsi_r.value(0);
            let stoch_value = stoch_r.value(0);
            if rsi_value > 1. - 0.35 && stoch_value > 1. - 0.35 {
                self.main_signal = Some(Action::BUY_ALL);
            } else if rsi_value < 0.35 && stoch_value < 0.35 {
                self.main_signal = Some(Action::SELL_ALL);
            }
            self.value = Some(SotchRsiValue {
                rsi: rsi_value,
                stoch: stoch_value,
                macd: macd_r.value(0),
            });
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
                    if pos.is_short() && (maybe_stop.is_some() || !matches!(self.main_signal, Some(Action::Sell(_)))) {
                        Some(self.make_signal(
                            le.trace_id,
                            le.ts,
                            OperationKind::Close,
                            PositionKind::Short,
                            final_candle.close,
                            None,
                        ))
                    }
                    // Possibly close a long position
                    else if pos.is_long()
                        && (maybe_stop.is_some() || !matches!(self.main_signal, Some(Action::Buy(_))))
                    {
                        Some(self.make_signal(
                            le.trace_id,
                            le.ts,
                            OperationKind::Close,
                            PositionKind::Long,
                            final_candle.close,
                            None,
                        ))
                    } else {
                        None
                    }
                }
                None if matches!(self.main_signal, Some(Action::Sell(_))) => {
                    // Possibly open a short position
                    let qty = Some(portfolio.value() / final_candle.close);
                    Some(self.make_signal(
                        le.trace_id,
                        le.ts,
                        OperationKind::Open,
                        PositionKind::Short,
                        final_candle.close,
                        qty,
                    ))
                }
                None if matches!(self.main_signal, Some(Action::Buy(_))) => {
                    // Possibly open a long position
                    let qty = Some(portfolio.value() / final_candle.close);
                    Some(self.make_signal(
                        le.trace_id,
                        le.ts,
                        OperationKind::Open,
                        PositionKind::Long,
                        final_candle.close,
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
        Ok(None)
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
            MarketEvent::CandleTick(c) if c.is_final => {
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
            .build()]
        .into_iter()
        .collect()
    }
}

#[cfg(test)]
mod test {
    use crate::rsistoch_strategy::{Options, StochRsiStrategy};
    use backtest::DatasetCatalog;
    use brokers::exchange::Exchange;
    use brokers::types::SecurityType;
    use chrono::{DateTime, NaiveDate, Utc};
    use stats::kline::{Resolution, TimeUnit};
    use std::sync::Arc;
    use strategy::driver::StratProviderRef;
    use util::time::DateRange;

    fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

    #[test]
    fn backtest() {
        init();
        actix::System::with_tokio_rt(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Default Tokio runtime could not be created.")
        })
        .block_on(async {
            let resolution = Resolution::new(TimeUnit::Minute, 1);
            let provider: StratProviderRef = Arc::new(move |_ctx| {
                Box::new(
                    StochRsiStrategy::try_new(
                        &Options {
                            exchange: Exchange::Binance,
                            pair: "BTC_USDT".into(),
                            resolution,
                            stop_loss: Some(-0.01),
                            trailing_stop_start: Some(0.01),
                            trailing_stop_loss: Some(0.002),
                            security_type: SecurityType::Future,
                            ..Options::default()
                        },
                        None,
                    )
                    .unwrap(),
                )
            });
            let mut report = backtest::backtest_with_range(
                "rsistoch_btc",
                provider,
                DateRange::by_day(
                    DateTime::from_utc(NaiveDate::from_ymd(2022, 2, 16).and_hms(0, 0, 0), Utc),
                    DateTime::from_utc(NaiveDate::from_ymd(2022, 2, 17).and_hms(0, 0, 0), Utc),
                ),
                &[Exchange::Binance],
                10000.0,
                0.001,
                Some(DatasetCatalog::default_prod()),
            )
            .await
            .unwrap();
            report.write_html();
            for table in &["snapshots", "models", "candles", "events"] {
                report.events_as_df(table).unwrap();
            }
        });
    }
}
